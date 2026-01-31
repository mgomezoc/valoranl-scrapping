import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)


@dataclass
class ParsedProperty:
    external_id: str
    title: str
    url: str
    image_url: Optional[str]
    price: Optional[int]
    currency: Optional[str]
    bedrooms: Optional[int]
    bathrooms: Optional[int]
    parking: Optional[int]
    built_m2: Optional[float]
    lot_m2: Optional[float]
    colony: Optional[str]
    city: Optional[str]
    raw: Dict[str, Any]


class Casas365Parser:
    """
    Parser de cards para Casas365.mx (theme wpresidence/wpestate).
    Basado estrictamente en el HTML que compartiste.
    """

    CARD_SELECTOR_PRIMARY = "div.listing_wrapper.property_unit_type7[data-listid]"
    CARD_SELECTOR_FALLBACK = "div.listing_wrapper[data-listid]"

    def extract_cards(self, soup: BeautifulSoup) -> List[Any]:
        cards = soup.select(self.CARD_SELECTOR_PRIMARY)
        if cards:
            return cards

        cards = soup.select(self.CARD_SELECTOR_FALLBACK)
        return cards

    def parse_card(self, card: Any) -> Optional[ParsedProperty]:
        try:
            external_id = (card.get("data-listid") or "").strip()
            if not external_id:
                logger.debug("Card sin data-listid, se omite.")
                return None

            # URL: prioriza data-link del contenedor interno
            url = None
            inner = card.select_one("div.property_listing[data-link]")
            if inner and inner.get("data-link"):
                url = inner.get("data-link", "").strip()

            if not url:
                a = card.select_one("h4 a[href]")
                if a and a.get("href"):
                    url = a.get("href", "").strip()

            if not url:
                logger.debug("Card %s sin URL, se omite.", external_id)
                return None

            # Título
            title = self._safe_text(card.select_one("h4 a")) or ""
            title = self._clean_ws(title)
            if not title:
                # fallback: data-modal-title existe en el wrapper
                title = self._clean_ws((card.get("data-modal-title") or "").strip())

            # Imagen principal
            image_url = None
            img = card.select_one("div.listing-unit-img-wrapper img")
            if img:
                image_url = (img.get("data-original") or img.get("src") or "").strip() or None

            # Precio + moneda
            price, currency = self._parse_price(card)

            # Características por tooltip
            bedrooms = self._parse_int_by_tooltip(card, "Recámaras")
            bathrooms = self._parse_int_by_tooltip(card, "Baños")
            built_m2 = self._parse_float_m2_by_tooltip(card, "Construcción")
            lot_m2 = self._parse_float_m2_by_tooltip(card, "Lot Size")

            # Estacionamientos: NO aparece como icono en este HTML.
            parking = None

            # Ubicación (best-effort) desde título: "Casa en Venta Portales de Lincoln, García"
            colony, city = self._extract_location_from_title(title)

            raw = {
                "data_listid": external_id,
                "data_modal_title": card.get("data-modal-title"),
                "data_modal_link": card.get("data-modal-link"),
            }

            return ParsedProperty(
                external_id=external_id,
                title=title,
                url=url,
                image_url=image_url,
                price=price,
                currency=currency,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                parking=parking,
                built_m2=built_m2,
                lot_m2=lot_m2,
                colony=colony,
                city=city,
                raw=raw,
            )

        except Exception:
            logger.exception("Error parseando card CASAS365.")
            return None

    # -------------------------
    # Helpers
    # -------------------------

    def _parse_price(self, card: Any) -> Tuple[Optional[int], Optional[str]]:
        """
        HTML observado:
        <div class="listing_unit_price_wrapper">
            $800,000 <span class="price_label">MXN</span>
        </div>
        """
        price_wrap = card.select_one("div.listing_unit_price_wrapper")
        if not price_wrap:
            return None, None

        # currency
        currency = self._safe_text(price_wrap.select_one("span.price_label"))
        currency = self._clean_ws(currency) if currency else None

        # price text: puede traer "$800,000" + espacios
        text = self._clean_ws(price_wrap.get_text(" ", strip=True))
        # elimina moneda textual para aislar números
        if currency:
            text = text.replace(currency, "")

        # extraer número con separadores
        m = re.search(r"(\$?\s*[\d.,]+)", text)
        if not m:
            return None, currency

        num_raw = m.group(1)
        num_raw = num_raw.replace("$", "").strip()
        # mx usual: "800,000"
        digits = re.sub(r"[^\d]", "", num_raw)
        if not digits:
            return None, currency

        try:
            return int(digits), currency
        except ValueError:
            return None, currency

    def _parse_int_by_tooltip(self, card: Any, tooltip_title: str) -> Optional[int]:
        """
        HTML observado:
        <div class="property_listing_details_v7_item" data-bs-original-title="Recámaras"> ... 2 </div>
        """
        sel = f'div.property_listing_details_v7_item[data-bs-original-title="{tooltip_title}"]'
        node = card.select_one(sel)
        if not node:
            return None

        text = self._clean_ws(node.get_text(" ", strip=True))
        # normalmente el número queda al final
        m = re.search(r"(\d+)\s*$", text)
        if not m:
            return None

        try:
            return int(m.group(1))
        except ValueError:
            return None

    def _parse_float_m2_by_tooltip(self, card: Any, tooltip_title: str) -> Optional[float]:
        """
        HTML observado:
        <div ... title="Construcción"><span>49.00 m<sup>2</sup></span></div>
        """
        sel = f'div.property_listing_details_v7_item[data-bs-original-title="{tooltip_title}"] span'
        node = card.select_one(sel)
        if not node:
            return None

        text = self._clean_ws(node.get_text(" ", strip=True))
        # ejemplos: "49.00 m2", "110.89 m2"
        m = re.search(r"([\d.]+)", text)
        if not m:
            return None

        try:
            return float(m.group(1))
        except ValueError:
            return None

    def _extract_location_from_title(self, title: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Best-effort con patrón visto:
        'Casa en Venta Portales de Lincoln, García'
        => colony='Portales de Lincoln', city='García'

        Si no matchea, devuelve (None, None).
        """
        if not title:
            return None, None

        # Busca el último ", {ciudad}"
        if "," not in title:
            return None, None

        left, right = title.rsplit(",", 1)
        city = self._clean_ws(right)
        if not city:
            return None, None

        # quita prefijos comunes del lado izquierdo
        # 'Casa en Venta ' / 'Departamento en Venta ' etc.
        left = self._clean_ws(left)
        left = re.sub(r"^(Casa|Departamento|Terreno|Bodega|Oficina)\s+en\s+(Venta|Renta)\s+", "", left, flags=re.I).strip()

        colony = self._clean_ws(left) or None
        return colony, city

    @staticmethod
    def _safe_text(node: Any) -> Optional[str]:
        if not node:
            return None
        return node.get_text(" ", strip=True)

    @staticmethod
    def _clean_ws(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "")).strip()


def build_casas365_page_url(base_url: str, page: int, mode: str = "pagina") -> str:
    """
    Generador de URL de paginación (lógica crítica).
    mode:
      - 'pagina' -> agrega/actualiza &pagina=page
      - 'paged'  -> agrega/actualiza &paged=page
    """
    if page < 1:
        page = 1

    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query, keep_blank_values=True)

    if mode == "paged":
        qs["paged"] = [str(page)]
        qs.pop("pagina", None)
    else:
        qs["pagina"] = [str(page)]
        qs.pop("paged", None)

    new_query = urlencode(qs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

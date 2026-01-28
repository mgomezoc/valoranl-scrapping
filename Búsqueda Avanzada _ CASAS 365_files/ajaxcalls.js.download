/*global $, jQuery, ajaxcalls_vars, document, control_vars, window, control_vars,Modernizr,mapfunctions_vars,wpestate_open_menu,grecaptcha,wpestate_show_no_results,wpestate_load_on_demand_pins,get_custom_value,wpestate_enable_half_map_pin_action,wpestate_lazy_load_carousel_property_unit,Chart,widgetId1,widgetId2,widgetId3,widgetId4,wpestate_get_custom_value_tab_search*/
///////////////////////////////////////////////////////////////////////////////////////////

function wpestate_load_stats_tabs(listing_id) {
    "use strict";
    var ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
    var nonce = jQuery('#wpestate_tab_stats').val();
    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        dataType: 'json',
        data: {
            'action': 'wpestate_load_stats_property',
            'postid': listing_id,
            'security': nonce,
        },
        success: function (data) {
            wpestate_show_prop_stat_graph_tab(data.array_values, data.labels, listing_id);
        },
        error: function (errorThrown) {}
    });//end ajax
}



function wpestate_show_prop_stat_graph_tab(values, labels, listing_id) {
    if (!document.getElementById('myChart')) {
        return;
    }

    var ctx = jQuery("#myChart").get(0).getContext("2d");
    var myNewChart = new Chart(ctx);
    // var labels      =   '';
    var traffic_data = '  ';

    // labels          =   jQuery.parseJSON ( wpestate_property_vars.singular_label);
    traffic_data = values;

    var data = {
        labels: labels,
        datasets: [
            {
                label: ajaxcalls_vars.property_views,
                fillColor: "rgba(220,220,220,0.5)",
                strokeColor: "rgba(220,220,220,0.8)",
                highlightFill: "rgba(220,220,220,0.75)",
                highlightStroke: "rgba(220,220,220,1)",
                data: traffic_data
            },
        ]
    };

    var options = {
        title: 'page views',
        //Boolean - Whether the scale should start at zero, or an order of magnitude down from the lowest value
        scaleBeginAtZero: true,

        //Boolean - Whether grid lines are shown across the chart
        scaleShowGridLines: true,

        //String - Colour of the grid lines
        scaleGridLineColor: "rgba(0,0,0,.05)",

        //Number - Width of the grid lines
        scaleGridLineWidth: 1,

        //Boolean - Whether to show horizontal lines (except X axis)
        scaleShowHorizontalLines: true,

        //Boolean - Whether to show vertical lines (except Y axis)
        scaleShowVerticalLines: true,

        //Boolean - If there is a stroke on each bar
        barShowStroke: true,

        //Number - Pixel width of the bar stroke
        barStrokeWidth: 2,

        //Number - Spacing between each of the X value sets
        barValueSpacing: 5,

        //Number - Spacing between data sets within X values
        barDatasetSpacing: 1,

        //String - A legend template
        legendTemplate: "<ul class=\"<%=name.toLowerCase()%>-legend\"><% for (var i=0; i<datasets.length; i++){%><li><span style=\"background-color:<%=datasets[i].fillColor%>\"></span><%if(datasets[i].label){%><%=datasets[i].label%><%}%></li><%}%></ul>"

    };

    // var myBarChart = new Chart(ctx).Bar(data, options);
    var myBarChart = new Chart(ctx, {
        type: 'bar',
        data: data,
        options: options
    });
}

/**
 * Remaps half-map listing card column sizes for list-mode rendering.
 *
 * @param {string} markup - HTML string containing listing_wrapper elements.
 * @returns {string} Updated markup with transformed column classes and data-org values.
 */
function wpestate_half_map_remap_list_columns(markup) {



     if (typeof ajaxcalls_vars !== 'undefined' && ajaxcalls_vars.wp_estate_prop_unit === 'list') {

        return markup;
    }

    if (typeof markup !== 'string' || markup === '') {
        return markup;
    }

    var tempWrapper = jQuery('<div/>').html(markup);

    tempWrapper.find('.listing_wrapper').each(function () {
        var wrapper = jQuery(this);
        var originalClasses = wrapper.attr('class') || '';
        console.log ("originalClasses"+originalClasses); 
        var mdMatch = originalClasses.match(/col-md-(\d+)/);
        var originalOrg = wrapper.attr('data-org') || (mdMatch ? mdMatch[1] : '') || '6';

        // Persist the original column sizing so grid view can be restored later.
        var lgMatch = originalClasses.match(/col-lg-(\d+)/);
        var smMatch = originalClasses.match(/col-sm-(\d+)/);

        wrapper.attr('data-half-map-original-md-class', mdMatch ? mdMatch[0] : '');
        wrapper.attr('data-half-map-original-lg-class', lgMatch ? lgMatch[0] : '');
        wrapper.attr('data-half-map-original-sm-class', smMatch ? smMatch[0] : '');
        wrapper.attr('data-half-map-original-org', originalOrg);
        var targetOrg = originalOrg;
console.log('remap '+ originalOrg); 
        switch (originalOrg) {
        case '6':
            targetOrg = '3';
            break;
        case '4':
        case '3':
            targetOrg = '2';
            break;
        case '12':
            targetOrg = '12';
            break;
        default:
            targetOrg = originalOrg;
        }

        ['md', 'lg', 'sm'].forEach(function (breakpoint) {
            var regex = new RegExp('col-' + breakpoint + '-\\d+', 'g');
            var matches = (wrapper.attr('class') || '').match(regex);

            if (matches) {
                wrapper.removeClass(matches.join(' '));
            }

            wrapper.addClass('col-' + breakpoint + '-' + targetOrg);
        });

        wrapper.attr('data-org', targetOrg);
    });

    return tempWrapper.html();
}



function wpestate_load_stats(listing_id) {
    "use strict";
    var ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
    var nonce = jQuery('#wpestate_tab_stats').val();

    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        dataType: 'json',
        data: {
            'action': 'wpestate_load_stats_property',
            'postid': listing_id,
            'security': nonce,
        },
        success: function (data) {
            wpestate_show_prop_stat_graph(data.array_values, data.labels, listing_id);
        },
        error: function (errorThrown) {}
    });//end ajax
}

function wpestate_show_prop_stat_graph(values, labels, listing_id) {
    "use strict";

    var ctx = jQuery("#myChart_" + listing_id).get(0).getContext('2d');
    var myNewChart = new Chart(ctx);
    var data = {
        labels: labels,
        datasets: [
            {
                label: ajaxcalls_vars.property_views,
                fillColor: "rgba(220,220,220,0.5)",
                strokeColor: "rgba(220,220,220,0.8)",
                highlightFill: "rgba(220,220,220,0.75)",
                highlightStroke: "rgba(220,220,220,1)",
                data: values
            },
        ]
    };

    var options = {
        scaleBeginAtZero: true,
        scaleShowGridLines: true,
        scaleGridLineColor: "rgba(0,0,0,.05)",
        scaleGridLineWidth: 1,
        scaleShowHorizontalLines: true,
        scaleShowVerticalLines: true,
        barShowStroke: true,
        barStrokeWidth: 2,
        barValueSpacing: 5,
        barDatasetSpacing: 1,
        legendTemplate: "<ul class=\"<%=name.toLowerCase()%>-legend\"><% for (var i=0; i<datasets.length; i++){%><li><span style=\"background-color:<%=datasets[i].fillColor%>\"></span><%if(datasets[i].label){%><%=datasets[i].label%><%}%></li><%}%></ul>"
    };

    // var myBarChart = new Chart(ctx).Bar(data, options);
    var myBarChart = new Chart(ctx, {
        type: 'bar',
        data: data,
        options: options
    });
}


//////////////////////////////////////////////////////////////////////////////////////////////
/// ajax filtering on header search ; jslint checked
////////////////////////////////////////////////////////////////////////////////////////////
function wpestate_start_filtering_ajax(newpage) {
    "use strict";

    var action, category, city, area, rooms, baths, min_price, price_max, ajaxurl, postid, halfmap, all_checkers;
    action = jQuery('#adv_actions').attr('data-value');
    category = jQuery('#adv_categ').attr('data-value');
    city = jQuery('#advanced_city').attr('data-value');
    area = jQuery('#advanced_area').attr('data-value');
    rooms = parseFloat(jQuery('#adv_rooms').val(), 10);
    baths = parseFloat(jQuery('#adv_bath').val(), 10);
    min_price = parseFloat(jQuery('#price_low').val(), 10);
    price_max = parseFloat(jQuery('#price_max').val(), 10);
    postid = parseFloat(jQuery('#adv-search-1').attr('data-postid'), 10);
    ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
    var order = jQuery('#a_filter_order').attr('data-value');
    halfmap = 0;

    var nonce = jQuery('#wpestate_ajax_filtering').val();

    if (jQuery('#google_map_prop_list_sidebar').length) {
        halfmap = 1;
    }

    postid = 1;
    if (document.getElementById('search_wrapper')) {
        postid = parseInt(jQuery('#search_wrapper').attr('data-postid'), 10);
    }

    all_checkers = '';
    jQuery('.search_wrapper .extended_search_check_wrapper  input[type="checkbox"]').each(function () {
        if (jQuery(this).is(":checked")) {
            all_checkers = all_checkers + "," + jQuery(this).attr("name-title");
        }
    });

    halfmap = 0;

    if (jQuery('#google_map_prop_list_sidebar').length) {
        halfmap = 1;
    }

    var geo_lat = '';
    var geo_long = '';
    var geo_rad = '';

    if (jQuery("#geolocation_search").length > 0) {
        geo_lat = jQuery('#geolocation_lat').val();
        geo_long = jQuery('#geolocation_long').val();
        geo_rad = jQuery('#geolocation_radius').val();

    }
    jQuery('#listing_ajax_container').empty();
    jQuery('#listing_loader').show();
    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        dataType: 'json',
        data: {
            'action': 'wpestate_ajax_filter_listings_search',
            'action_values': action,
            'category_values': category,
            'city': city,
            'area': area,
            'advanced_rooms': rooms,
            'advanced_bath': baths,
            'price_low': min_price,
            'price_max': price_max,
            'newpage': newpage,
            'postid': postid,
            'halfmap': halfmap,
            'all_checkers': all_checkers,
            'geo_lat': geo_lat,
            'geo_long': geo_long,
            'geo_rad': geo_rad,
            'order': order,
            'security': nonce
        },
        success: function (data) {

            jQuery('#listing_loader').hide();
            jQuery('.listing_loader_title').show();
            jQuery('.pagination_nojax').hide();
            //jQuery('#listing_ajax_container').empty().append(data);
            jQuery('.entry-title.title_prop').addClass('half_results').text(data.no_founs);

            jQuery('#listing_ajax_container').empty().append(data.cards);


            wpestate_restart_js_after_ajax();

        },
        error: function (errorThrown) {}
    });//end ajax
}


//////////////////////////////////////////////////////////////////////////////////////////////
/// ajax filtering on header search ; jslint checked
////////////////////////////////////////////////////////////////////////////////////////////

function wpestate_custom_search_start_filtering_ajax(newpage) {
    "use strict";
console.log('wpestate_custom_search_start_filtering_ajax');

    var initial_array_last_item, temp_val, array_last_item, how_holder, slug_holder, val_holder, ajaxurl, postid, slider_min, slider_max, halfmap, all_checkers, term_id;

    array_last_item = parseInt(mapfunctions_vars.fields_no, 10);
    initial_array_last_item = array_last_item;

    val_holder = [];
    slug_holder = [];
    how_holder = [];


    //slug_holder         =   mapfunctions_vars.slugs;
    slug_holder = JSON.parse(JSON.stringify(mapfunctions_vars.slugs));

    // how_holder          =   mapfunctions_vars.hows;
    how_holder = JSON.parse(JSON.stringify(mapfunctions_vars.hows));
    var term_counter = 0;



    if (mapfunctions_vars.slider_price === 'yes') {
        slider_min = jQuery('#price_low').val();
        slider_max = jQuery('#price_max').val();
    }

    if(jQuery('.wpresidence-component3-min-price_input_class').length>0){
        slider_min = jQuery('.wpresidence-component3-min-price_input_class').val();
        slider_max = jQuery('.wpresidence-component3-max-price_input_class').val();
    }



    term_counter = jQuery('.tab-pane.active').find('.term_counter').val();


    if ((mapfunctions_vars.adv_search_type === '6' || mapfunctions_vars.adv_search_type === '7')) { // &&  !jQuery('.halfsearch')[0]

        term_id = jQuery('.tab-pane.active .term_id_class').val();


        if (mapfunctions_vars.slider_price === 'yes') {
            slider_min = jQuery('#price_low_' + term_id).val();
            slider_max = jQuery('#price_max_' + term_id).val();
        }


        if(jQuery('.tab-pane.active .wpresidence-component3-min-price_input_class').length>0){
            slider_min = jQuery('.tab-pane.active .wpresidence-component3-min-price_input_class').val();
            slider_max = jQuery('.tab-pane.active .wpresidence-component3-max-price_input_class').val();
        }

        

        var start_counter = term_counter * mapfunctions_vars.fields_no;
        if (term_counter > 0) {
            array_last_item = (array_last_item * term_counter) + array_last_item;
        }


        for (var i = start_counter; i < array_last_item; i++) {
            if (how_holder[i] == 'date bigger' || how_holder[i] == 'date smaller') {
                temp_val = wpestate_get_custom_value_tab_search(term_id + slug_holder[i]);
            } else {
                temp_val = wpestate_get_custom_value_tab_search(slug_holder[i]);
            }

            if (typeof (temp_val) === 'undefined') {
                temp_val = '';
            }
            val_holder.push(temp_val);
        }

    } else {
        for (var i = 0; i < array_last_item; i++) {
            temp_val = wpestate_get_custom_value(slug_holder[i]);
            if (typeof (temp_val) === 'undefined') {
                temp_val = '';
            }
            val_holder.push(temp_val);
        }

    }


    if ((mapfunctions_vars.adv_search_type === '6' || mapfunctions_vars.adv_search_type === '7' || mapfunctions_vars.adv_search_type === '8' || mapfunctions_vars.adv_search_type === '9')) {
        var tab_tax = jQuery('.adv_search_tab_item.active').attr('data-tax');

        if (jQuery('.halfsearch')[0]) {
            tab_tax = jQuery('.halfsearch').attr('data-tax');
        }


        if (tab_tax === 'property_category') {
            slug_holder[array_last_item] = 'adv_categ';
        } else if (tab_tax === 'property_action_category') {
            slug_holder[array_last_item] = 'adv_actions';
        } else if (tab_tax === 'property_city') {
            slug_holder[array_last_item] = 'advanced_city';
        } else if (tab_tax === 'property_area') {
            slug_holder[array_last_item] = 'advanced_area';
        } else if (tab_tax === 'property_county_state') {
            slug_holder[array_last_item] = 'county-state';
        }

        how_holder[array_last_item] = 'like';

        if (jQuery('.halfsearch')[0] && mapfunctions_vars.adv_search_type === '8') {
            val_holder[array_last_item] = jQuery('#' + slug_holder[array_last_item]).parent().find('input:hidden').val();
        } else {
            val_holder[initial_array_last_item] = jQuery('.adv_search_tab_item.active').attr('data-term');
        }

    }


    all_checkers = '';

    if (mapfunctions_vars.adv_search_type === '6') {
        jQuery('.tab-pane.active .extended_search_check_wrapper  input[type="checkbox"]').each(function () {
            if (jQuery(this).is(":checked")) {
                all_checkers = all_checkers + "," + jQuery(this).attr("name-title");
            }
        });
    } else {

        jQuery('.search_wrapper .extended_search_check_wrapper  input[type="checkbox"]').each(function () {
            if (jQuery(this).is(":checked")) {
                all_checkers = all_checkers + "," + jQuery(this).attr("name-title");
            }
        });
    }





    halfmap = 0;

    if (jQuery('#google_map_prop_list_sidebar').length) {
        halfmap = 1;
    }
    postid = 1;
    if (document.getElementById('search_wrapper')) {
        postid = parseInt(jQuery('#search_wrapper').attr('data-postid'), 10);
    }
    ajaxurl = ajaxcalls_vars.wpestate_ajax;



    var filter_search_action10 = '';
    var adv_location10 = '';

    if (mapfunctions_vars.adv_search_type === '10') {
        filter_search_action10 = jQuery('#adv_actions').attr('data-value');
        adv_location10 = jQuery('#adv_location').val();
    }


    var filter_search_action11 = '';
    var filter_search_categ11 = '';
    var keyword_search = '';
    keyword_search = jQuery('#keyword').val();
    if (mapfunctions_vars.adv_search_type === '11') {
        filter_search_action11 = jQuery('#adv_actions').attr('data-value');
        filter_search_categ11 = jQuery('#adv_categ').attr('data-value');
        keyword_search = jQuery('#keyword_search').val();
    }
    var geo_lat = '';
    var geo_long = '';
    var geo_rad = '';
    var componentsbeds = '';
    var componentsbaths= '';

    if (jQuery("#geolocation_search").length > 0) {
        geo_lat = jQuery('#geolocation_lat').val();
        geo_long = jQuery('#geolocation_long').val();
        geo_rad = jQuery('#geolocation_radius').val();

    }

    if (jQuery('.tab-pane.active .geolocation_search_item').length>0){
        
        geo_lat = jQuery('.tab-pane.active .geolocation_lat').val();
        geo_long = jQuery('.tab-pane.active .geolocation_long').val();
        geo_rad = jQuery('.tab-pane.active #geolocation_radius').val();
    }



    
    if (jQuery('.wpestate-beds-baths-popoup-component').length>0){
        componentsbeds = jQuery('.wpresidence-componentsbeds').val();
        componentsbaths = jQuery('.wpresidence-componentsbaths').val();
    }


    if (jQuery('.tab-pane.active .wpestate-beds-baths-popoup-component').length>0){
        componentsbeds = jQuery('.tab-pane.active .wpresidence-componentsbeds').val();
        componentsbaths = jQuery('.tab-pane.active .wpresidence-componentsbaths').val();
    }




    var nonce = jQuery('#wpestate_ajax_filtering').val();
    var order = jQuery('#a_filter_order').attr('data-value');


    const listingContainer = jQuery('#listing_ajax_container');
    wpestate_createSkeletons(listingContainer);


    //jQuery('#listing_ajax_container').empty();
//    jQuery('#listing_loader').show();
    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        dataType: 'json',
        data: {
            'action': 'wpestate_custom_adv_ajax_filter_listings_search',
            'val_holder': val_holder,
            'newpage': newpage,
            'postid': postid,
            'slider_min': slider_min,
            'slider_max': slider_max,
            'halfmap': halfmap,
            'all_checkers': all_checkers,
            'filter_search_action10': filter_search_action10,
            'adv_location10': adv_location10,
            'filter_search_action11': filter_search_action11,
            'filter_search_categ11': filter_search_categ11,
            'keyword_search': keyword_search,
            'geo_lat': geo_lat,
            'geo_long': geo_long,
            'geo_rad': geo_rad,
            'order': order,
            'term_counter': term_counter,
            'componentsbeds'            :   componentsbeds,
            'componentsbaths'           :   componentsbaths,
            'security': nonce,
        },
        success: function (data) {

    

            jQuery('#listing_loader').hide();
            jQuery('.listing_loader_title').show();
            jQuery('.entry-title.title_prop').addClass('half_results').text(data.no_founs);

            var cardsMarkup = data.cards;

            // When half-map list view is active, remap the returned column sizes
            // before injecting the new cards so they respect the list-mode sizing rules.
            if (window.wpestateHalfMapState &&
                typeof window.wpestateHalfMapState.getMode === 'function' &&
                window.wpestateHalfMapState.getMode() === 'list') {
                cardsMarkup = wpestate_half_map_remap_list_columns(cardsMarkup);
            }

            //  jQuery('#listing_ajax_container').empty().append(cardsMarkup);
            wpestate_replaceSkeletons(listingContainer, cardsMarkup);

            if (window.wpestateHalfMapState && typeof window.wpestateHalfMapState.applyCurrentLayout === 'function') {
                window.wpestateHalfMapState.applyCurrentLayout();
            }


            wpestate_restart_js_after_ajax();
            jQuery('.col-md-12.listing_wrapper .property_unit_custom_element.image').each(function () {
                jQuery(this).parent().addClass('wrap_custom_image');
            });
            
            if (data.saved_search !== null) {
                jQuery(".search_param").html(data.saved_search);
                jQuery("#search_args").val( JSON.stringify(data.args) );
                jQuery("#meta_args").val(  JSON.stringify(data.search_meta) );

            }
        },
        error: function (errorThrown) {

        }
    });//end ajax
}













function wpestate_js_pagination_ajax(){
    "use strict";

    jQuery('.pagination_ajax a').on('click', function (event) {
        event.preventDefault();
        var newpage = parseInt(jQuery(this).attr('data-future'), 10);
        wpestate_start_filtering(newpage);
    });

}


////////////////////////////////////////////////////////////////////////////////////////////
/// redo js after ajax calls - jslint checked
////////////////////////////////////////////////////////////////////////////////////////////
function wpestate_restart_js_after_ajax() {
    "use strict";
    wpestate_lazy_load_carousel_property_unit();
    wpestate_card_unit_contact_actions();


    // wpestate_enable_half_map_pin_action();
    if (typeof wpestate_enable_half_map_pin_action == 'function') {
        wpestate_enable_half_map_pin_action();
    }
    var newpage, post_id, post_image, to_add, icon;

    jQuery('.pagination_ajax_search a').on('click', function (event) {
        event.preventDefault();

        newpage = parseInt(jQuery(this).attr('data-future'), 10);
        document.getElementById('scrollhere').scrollIntoView();
        if (mapfunctions_vars.custom_search === 'yes') {
            wpestate_custom_search_start_filtering_ajax(newpage);  // should be custom
        } else {
            wpestate_start_filtering_ajax(newpage);//
        }
    });

    jQuery('.pagination_ajax a').on('click', function (event) {
        event.preventDefault();
        newpage = parseInt(jQuery(this).attr('data-future'), 10);
        document.getElementById('scrollhere').scrollIntoView();
        wpestate_start_filtering(newpage);
    });



    jQuery('.property_listing').on('click', function (event) {
        if (
            event.target.classList.contains('demo-icon') || 
            event.target.classList.contains('carousel-control-next-icon') || 
            event.target.classList.contains('carousel-control-prev-icon')   
        ){
            return;
        }


        if (control_vars.property_modal === '1' && !Modernizr.mq('only all and (max-width: 1024px)')) {
            event.preventDefault();
            event.stopPropagation();
            scroll_modal_save = scroll_modal;

            var listing_id = jQuery(this).parent().attr('data-listid');
            var main_img_url = jQuery(this).parent().attr('data-main-modal');
            var main_title = jQuery(this).parent().attr('data-modal-title');
            var link = jQuery(this).parent().attr('data-modal-link');
            wpestate_enable_property_modal(listing_id, main_img_url, main_title, link);
        } else {
            if (control_vars.new_page_link === '_blank') {
                return;
            }
            var link;
            link = jQuery(this).attr('data-link');

            window.open(link, '_self');
        }
    });





    jQuery('.share_unit').on('click', function (event) {
        event.stopPropagation();
    });

    var already_in = [];

    jQuery('.compare-action').unbind('click');
    jQuery('.compare-action').on('click', function (event) {
        event.stopPropagation();
        jQuery('.prop-compare').animate({
            right: "0px"
        });
        post_id = jQuery(this).attr('data-pid');
        for (var i = 0; i < already_in.length; i++) {
            if (already_in[i] === post_id) {
                return;
            }
        }

        already_in.push(post_id);
        post_image = jQuery(this).attr('data-pimage');

        to_add = '<div class="items_compare ajax_compare" style="display:none;"><img src="' + post_image + '" class="img-responsive"><input type="hidden" value="' + post_id + '" name="selected_id[]" /></div>';
        jQuery('div.items_compare:first-child').css('background', 'red');
        if (parseInt(jQuery('.items_compare').length, 10) > 3) {
            jQuery('.items_compare:first').remove();
        }
        jQuery('#submit_compare').before(to_add);
        jQuery('.items_compare').fadeIn(800);
    });

    jQuery('#submit_compare').unbind('click');
    jQuery('#submit_compare').on('click', function (event) {
        jQuery('#form_compare').trigger('submit');
    });

    jQuery('.icon-fav').on('click', function (event) {
        event.stopPropagation();
        icon = jQuery(this);
        wpestate_add_remove_favorite(icon);
    });

    jQuery(".share_list, .icon-fav, .compare-action,.property_listing_details_v7_item").on('mouseenter', function () {
        jQuery(this).tooltip('show');
    })
            .on('mouseleave', function () {
                jQuery(this).tooltip('hide');
            });


    jQuery('.share_list').on('click', function (event) {
        event.stopPropagation();
        var sharediv = jQuery(this).parent().find('.share_unit');
        sharediv.toggle();
        jQuery(this).toggleClass('share_on');
    });

    wpestate_grid_list_controls();
    wpestate_enable_share_unit();
    wpestate_enable_not_logged_favorites();
}





/*
*
* Remove/Add to favorites(property page)  when login is not required
*
*/

function wpestate_add_remove_favorite_local_storage_single(icon,post_id){
    var current_favorites = window.localStorage.getItem('wpestate_favorites');
    post_id=parseInt(post_id);


    if(icon.hasClass('isnotfavorite')){

        if ( current_favorites ) {
            current_favorites=current_favorites + ',' + post_id;
            window.localStorage.setItem('wpestate_favorites',current_favorites );
        } else {
            current_favorites=post_id;
            window.localStorage.setItem('wpestate_favorites', current_favorites);
        }

        setCookie('wpestate_favorites',current_favorites,10);

        jQuery('#add_favorites').removeClass('isnotfavorite').addClass('isfavorite').attr('title', ajaxcalls_vars.remove_favorite);
        jQuery('#add_favorites').html('<i class="fas fa-heart"></i>' + ajaxcalls_vars.favorite);
  
    }else{

        var prop_ids = jQuery.map(current_favorites.split(','), function(value){
						return parseInt(value);
					});

               
        const index = prop_ids.indexOf( post_id );      
      
        if (index > -1) {
            prop_ids.splice(index, 1);      
            window.localStorage.setItem('wpestate_favorites', prop_ids);
            setCookie('wpestate_favorites',prop_ids,10);
        }


        jQuery('#add_favorites').removeClass('isfavorite').addClass('isnotfavorite').attr('title', ajaxcalls_vars.add_favorite);
        jQuery('#add_favorites').html('<i class="far fa-heart"></i>' + ajaxcalls_vars.favorite);

    }

    current_favorites = window.localStorage.getItem('wpestate_favorites');
}



/*
*
* Remove/Add to favorites when login is not required
*
*/


function wpestate_add_remove_favorite_local_storage(icon,post_id){
    var current_favorites = window.localStorage.getItem('wpestate_favorites');
    post_id=parseInt(post_id);

    if(icon.hasClass('icon-fav-off')){

        if ( current_favorites ) {
            current_favorites=current_favorites+','+post_id;
            window.localStorage.setItem('wpestate_favorites', current_favorites);
        } else {
            current_favorites=post_id;
            window.localStorage.setItem('wpestate_favorites', current_favorites);
        }

        setCookie('wpestate_favorites',current_favorites,10);

        icon.removeClass('icon-fav-off').addClass('icon-fav-on');
        icon.attr('title', ajaxcalls_vars.remove_fav);

    }else{

        var prop_ids = jQuery.map(current_favorites.split(','), function(value){
						return parseInt(value);
					});
           
        const index = prop_ids.indexOf( post_id );      
  
        if (index > -1) {
            prop_ids.splice(index, 1);
            window.localStorage.setItem('wpestate_favorites', prop_ids);
            setCookie('wpestate_favorites',prop_ids,10);
        }

        if (icon.hasClass('remove_fav_dash')) {
            icon.parents('.property_wrapper_dash').remove();
            icon.parents('.listing_wrapper').remove();
        }

        icon.removeClass('icon-fav-on').addClass('icon-fav-off');
        icon.attr('title', ajaxcalls_vars.add_favorite);
    }

    current_favorites = window.localStorage.getItem('wpestate_favorites');
}



/*
*
* Remove/Add to favorites
*
*/

function wpestate_add_remove_favorite(icon) {
    "use strict";
    var post_id, securitypass, ajaxurl;
    post_id = icon.attr('data-postid');
    securitypass = jQuery('#security-pass').val();
    ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
    var nonce = jQuery('#wpestate_ajax_filtering').val();
    var use_favorite_login = ajaxcalls_vars.favorites_login;
    
    if(use_favorite_login == "no"){
        wpestate_add_remove_favorite_local_storage(icon, post_id);
        return;
    }
    
    if (parseInt(ajaxcalls_vars.userid, 10) === 0) {
        wpestate_trigger_login_modal_open(1);
    } else {
        icon.toggleClass('icon-fav-off');
        icon.toggleClass('icon-fav-on');

        
        jQuery.ajax({
            type: 'POST',
            url: ajaxurl,
            dataType: 'json',
            data: {
                'action': 'wpestate_ajax_add_fav',
                'post_id': post_id,
                'security': nonce
            },
            success: function (data) {
         
                if (data.added) {
                    icon.removeClass('icon-fav-off').addClass('icon-fav-on');
                    // Update Bootstrap tooltip attributes
                    icon.attr('aria-label', ajaxcalls_vars.remove_fav);
                    icon.attr('data-bs-original-title', ajaxcalls_vars.remove_fav);
                    // Also update title for fallback
                    icon.attr('title', ajaxcalls_vars.remove_fav);
                    
                    // Refresh the Bootstrap tooltip if it exists
                    if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
                        var tooltipInstance = bootstrap.Tooltip.getInstance(icon[0]);
                        if (tooltipInstance) {
                            tooltipInstance.setContent({
                                '.tooltip-inner': ajaxcalls_vars.remove_fav
                            });
                        }
                    }
                } else {
                    if (icon.hasClass('remove_fav_dash')) {
                        icon.parents('.property_wrapper_dash').remove();
                    }
                    icon.removeClass('icon-fav-on').addClass('icon-fav-off');
                    // Update Bootstrap tooltip attributes
                    icon.attr('aria-label', ajaxcalls_vars.add_favorite);
                    icon.attr('data-bs-original-title', ajaxcalls_vars.add_favorite);
                    // Also update title for fallback
                    icon.attr('title', ajaxcalls_vars.add_favorite);
                    
                    // Refresh the Bootstrap tooltip if it exists
                    if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
                        var tooltipInstance = bootstrap.Tooltip.getInstance(icon[0]);
                        if (tooltipInstance) {
                            tooltipInstance.setContent({
                                '.tooltip-inner': ajaxcalls_vars.add_favorite
                            });
                        }
                    }
                }
            },
            error: function (errorThrown) {
                // Handle error case
            }
        }); // end ajax
    } // end login use
}

////////////////////////////////////////////////////////////////////////////////////////////
/// resend listing for approval-jslint checked
////////////////////////////////////////////////////////////////////////////////////////////
function wpestate_resend_for_approval(prop_id, selected_div) {
    "use strict";
    var ajaxurl, normal_list_no;
    ajaxurl = control_vars.admin_url + 'admin-ajax.php';
    var nonce = jQuery('#wpestate_property_actions').val();

    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        data: {
            'action': 'wpestate_ajax_resend_for_approval',
            'propid': prop_id,
            'security': nonce
        },
        success: function (data) {

            if (data === 'pending') {
                selected_div.parent().empty().append('<span class="featured_prop">Sent for approval</span>');
                normal_list_no = parseInt(jQuery('#normal_list_no').text(), 10);
                jQuery('#normal_list_no').text(normal_list_no - 1);
            } else {
                selected_div.removeClass('resend_pending').html(data);
            }
        },
        error: function (errorThrown) {

        }
    });//end ajax
}

////////////////////////////////////////////////////////////////////////////////////////////
/// make property featured-jslint checked
////////////////////////////////////////////////////////////////////////////////////////////
function wpestate_make_prop_featured(prop_id, selectedspan) {
    "use strict";
    var ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
    var nonce = jQuery('#wpestate_property_actions').val();

    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        data: {
            'action': 'wpestate_ajax_make_prop_featured',
            'propid': prop_id,
            'security': nonce
        },
        success: function (data) {
            if (data.trim() === 'done') {
                selectedspan.empty().html('<span class="label label-success">' + ajaxcalls_vars.prop_featured + '</span>');
                var featured_list_no = parseInt(jQuery('#featured_list_no').text(), 10);
                jQuery('#featured_list_no').text(featured_list_no - 1);
            } else {
                selectedspan.empty().removeClass('make_featured').addClass('featured_exp').text(ajaxcalls_vars.no_prop_featured);
            }

        },
        error: function (errorThrown) {
        }

    });//end ajax
}

////////////////////////////////////////////////////////////////////////////////////////////
/// pay package via paypal recuring-jslint checked
////////////////////////////////////////////////////////////////////////////////////////////
function wpestate_recuring_pay_pack_via_paypal() {
    "use strict";
    var ajaxurl, packName, packId;
    ajaxurl = control_vars.admin_url + 'admin-ajax.php';

    packName = jQuery('.package_selected .pack-listing-title').text();
    packId = jQuery('.package_selected .pack-listing-title').attr('data-packid');

    var nonce = jQuery('#wpestate_payments_nonce').val();
    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        data: {
            'action': 'wpestate_ajax_paypal_pack_recuring_generation_rest_api',
            'packName': packName,
            'packId': packId,
            'security': nonce
        },
        success: function (data) {
            window.location.href = data;
        },
        error: function (errorThrown) {

        }
    });//end ajax
}

jQuery(".buypackage").click(function () {
    "use strict";
    var stripe_pack_id, stripetitle2, stripetitle, stripepay;


    jQuery(".pack-listing").removeClass("package_selected");
    jQuery(".pack-listing input").prop('checked', false);

    jQuery(this).find('.input_pak_check').prop('checked', true);
    jQuery(this).parent().addClass("package_selected");

    stripetitle = jQuery(this).parent().find('.pack-listing-title').attr('data-stripetitle');
    stripetitle2 = jQuery(this).parent().find('.pack-listing-title').attr('data-stripetitle2');
    stripepay = jQuery(this).parent().find('.pack-listing-title').attr('data-stripepay');
    stripe_pack_id = jQuery(this).parent().find('.pack-listing-title').attr('data-packid');

    jQuery('.stripe_buttons').attr("id", stripetitle2);
    jQuery('#stripe_script').attr("data-amount", stripepay);
    jQuery('#stripe_script').attr("data-description", stripetitle);

    jQuery('#pack_id').val(stripe_pack_id);
    jQuery('#pack_title').val(stripetitle2);
    jQuery('#pay_ammout').val(stripepay);
    jQuery('#stripe_form').attr('data-amount', stripepay);


    jQuery('.wpestate_stripe_pay_desc').html(control_vars.stripe_pay_for + " " + stripetitle2);
    jQuery('#wpestate_stripe_form_button_sumit').html(control_vars.stripe_pay + " " + stripepay / 100 + " " + control_vars.submission_curency);

    // enable stripe code


});



////////////////////////////////////////////////////////////////////////////////////////////
/// pay package via paypal-jslint checked
////////////////////////////////////////////////////////////////////////////////////////////
function wpestate_pay_pack_via_paypal() {
    "use strict";
    var ajaxurl, packName, packId;
    ajaxurl = control_vars.admin_url + 'admin-ajax.php';
    packName = jQuery('.package_selected .pack-listing-title').text();
    packId = jQuery('.package_selected .pack-listing-title').attr('data-packid');
    var nonce = jQuery('#wpestate_payments_nonce').val();
    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        data: {
            'action': 'wpestate_ajax_paypal_pack_generation',
            'packName': packName,
            'packId': packId,
            'security': nonce
        },
        success: function (data) {

            window.location.href = data;
        },
        error: function (errorThrown) {

        }
    });//end ajax

}
////////////////////////////////////////////////////////////////////////////////////////////
/// listing pay -jslint checked
////////////////////////////////////////////////////////////////////////////////////////////
function wpestate_listing_pay(prop_id, selected_div, is_featured, is_upgrade) {
    "use strict";
    var ajaxurl = control_vars.admin_url + 'admin-ajax.php';
    var nonce = jQuery('#wpestate_payments_nonce').val();
    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        data: {
            'action': 'wpestate_ajax_listing_pay',
            'propid': prop_id,
            'is_featured': is_featured,
            'is_upgrade': is_upgrade,
            'security': nonce
        },
        success: function (data) {
            window.location.href = data;
        },
        error: function (errorThrown) {
        }
    });//end ajax
}

////////////////////////////////////////////////////////////////////////////////////////////
/// start filtering -jslint checked
////////////////////////////////////////////////////////////////////////////////////////////
function wpestate_start_filtering(newpage) {
    "use strict";
    jQuery('#gmap-loading').show();
    jQuery('#grid_view').addClass('icon_selected');
    jQuery('#list_view').removeClass('icon_selected');

    var action, category, status, features, county, city, area, order, ajaxurl, page_id;
    // get action vars
    action = jQuery('#a_filter_action').attr('data-value');
    // get category
    category = jQuery('#a_filter_categ').attr('data-value');

    // get status
    status = jQuery('#a_filter_status').attr('data-value');

    // get features
    features = jQuery('#a_filter_features').attr('data-value');

    // get county
    county = jQuery('#a_filter_county').attr('data-value');

    // get city
    city = jQuery('#a_filter_cities').attr('data-value');
    // get area
    area = jQuery('#a_filter_areas').attr('data-value');
    // get order
    order = jQuery('#a_filter_order').attr('data-value');
    ajaxurl = ajaxcalls_vars.wpestate_ajax;
    page_id = jQuery('#page_idx').val();

    var card_version = jQuery('.wpestate_filter_list_properties_wrapper').attr('data-card_version');
    var rownumber = jQuery('.wpestate_filter_list_properties_wrapper').attr('data-rownumber');
    var ishortcode = jQuery('.wpestate_filter_list_properties_wrapper').attr('data-ishortcode');
    var number = jQuery('.wpestate_filter_list_properties_wrapper').attr('data-number');
    var context = jQuery('.wpestate_filter_list_properties_wrapper').attr('data-context');



    var align = '';
    if (jQuery('.wpestate_filter_list_properties_wrapper').length > 0) {

        wpestate_createSkeletons(jQuery('.wpestate_filter_list_properties_wrapper'));



        //jQuery('.wpestate_filter_list_properties_wrapper .listing_wrapper').remove();
        //jQuery('.wpestate_filter_list_properties_wrapper .pagination_ajax').remove();
        //jQuery('.wpestate_filter_list_properties_wrapper .no_results').remove();
        align = jQuery('.wpestate_filter_list_properties_wrapper').attr('data-align');

        //jQuery('#listing_loader2').show();
    } else {
        //jQuery('#listing_ajax_container').empty();
         wpestate_createSkeletons(jQuery('#listing_ajax_container'));
//       jQuery('#listing_loader').show();

    }
    var nonce = jQuery('#wpestate_ajax_filtering').val();

    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        dataType: 'json',
        data: {
            'action': 'wpestate_ajax_filter_listings',
            'action_values': action,
            'category_values': category,
            'status': status,
            'features': features,
            'county': county,
            'city': city,
            'area': area,
            'order': order,
            'newpage': newpage,
            'page_id': page_id,
            'align': align,
            'card_version': card_version,
            'rownumber': rownumber,
            'ishortcode': ishortcode,
            'number':number,
            'context':context,
            'security': nonce
        },
        success: function (data) {

            jQuery('#listing_loader').hide();
            if (jQuery('.wpestate_filter_list_properties_wrapper').length > 0) {
              //  jQuery('#listing_loader2').hide();
                //jQuery('.wpestate_filter_list_properties_wrapper').append(data.to_show);

                  wpestate_replaceSkeletons(jQuery('.wpestate_filter_list_properties_wrapper'), data.to_show);
            } else {
               // jQuery('#listing_ajax_container').empty().append(data.to_show);
                 wpestate_replaceSkeletons(jQuery('#listing_ajax_container'), data.to_show);
            }

            jQuery('.pagination_nojax').hide();
          //  wpestate_restart_js_after_ajax();

            // map update
            var no_results = parseInt(data.no_results);
            if (no_results !== 0) {
                wpestate_load_on_demand_pins(data.markers, no_results, 0);
            } else {
                wpestate_show_no_results();
            }

            jQuery('.col-md-12.listing_wrapper .property_unit_custom_element.image').each(function () {
                jQuery(this).parent().addClass('wrap_custom_image');
            });
 
            jQuery('#gmap-loading').hide();

        },
        error: function (errorThrown) {

        }
    });//end ajax
}







////////////////////////////////////////////////////////////////////////////////////////////
// on ready -jslint checked
//  developer listing on tab click !!
////////////////////////////////////////////////////////////////////////////////////////////
jQuery(document).ready(function ($) {
    "use strict";

    wpestate_add_to_favorites();
    wpestate_js_pagination_ajax();
    wpestate_enable_not_logged_favorites();
   // wpestate_loade_agent_listings();
    wpestate_load_agency_developer_listings();


    wpestate_agent_submit_email();

    wpestate_theme_slider_show_contact();
    wpestate_enable_schedule_contact();
   





    ///////////////////////////////////////////////////////////////////////////////////////////
    //// stripe cancel
    ///////////////////////////////////////////////////////////////////////////////////////////
    $('#stripe_cancel').on('click', function (event) {

        var stripe_user_id, ajaxurl;
        stripe_user_id = $(this).attr('data-stripeid');
        ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
        $('#stripe_cancel').text(ajaxcalls_vars.saving);
        var nonce = jQuery('#wpestate_stripe_cancel_nonce').val();

        $.ajax({
            type: 'POST',
            url: ajaxurl,
            data: {
                'action': 'wpestate_cancel_stripe',
                'stripe_customer_id': stripe_user_id,
                'security': nonce,

            },
            success: function (data) {
                $('#stripe_cancel').text(ajaxcalls_vars.stripecancel);
            },
            error: function (errorThrown) {
            }
        });
    });


    ////////////////////////////////////////////////////////////////////////////////////////////
    /// resend for approval
    ///////////////////////////////////////////////////////////////////////////////////////////
    $('.resend_pending').on('click', function (event) {

        var prop_id = $(this).attr('data-listingid');
        wpestate_resend_for_approval(prop_id, $(this));
    });



    jQuery('#wpestate_stripe_booking_recurring').on('click', function () {

        var modalid = jQuery(this).attr('data-modalid');
        var modal = jQuery('#' + modalid);
        
        modal.appendTo('body');
        
        modal.show();
        jQuery('#' + modalid + ' .wpestate_stripe_form_1').show();

        wpestate_start_stripe(1, modalid);
    });

    ///////////////////////////////////////////////////////////////////////////////////////////
    ////////  pack upgrade via paypal
    ///////////////////////////////////////////////////////////////////////////////////////////
    $('#pick_pack').on('click', function (event) {

        var pay_paypal;
        pay_paypal = '<div class="modal fade" id="paypal_modal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true"><div class="modal-dialog"><div class="modal-content"><div class="modal-body listing-submit">' + ajaxcalls_vars.paypal + '</div></div></div></div></div>';
        jQuery('body').append(pay_paypal);
        jQuery('#paypal_modal').modal();


        if ($('#pack_recuring').is(':checked')) {
            wpestate_recuring_pay_pack_via_paypal();
        } else {
            wpestate_pay_pack_via_paypal();
        }
    });

    ///////////////////////////////////////////////////////////////////////////////////////////
    //////// listing pay via paypal
    ///////////////////////////////////////////////////////////////////////////////////////////
    $('.listing_submit_normal').on('click', function (event) {
        var prop_id, featured_checker, is_featured, is_upgrade, pay_paypal;
        pay_paypal = '<div class="modal fade" id="paypal_modal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true"><div class="modal-dialog"><div class="modal-content"><div class="modal-body listing-submit">' + ajaxcalls_vars.paypal + '</div></div></div></div></div>';
        jQuery('body').append(pay_paypal);
        jQuery('#paypal_modal').modal();


        prop_id = $(this).attr('data-listingid');
        featured_checker = $(this).parent().find('input');
        is_featured = 0;
        is_upgrade = 0;

        if (featured_checker.prop('checked')) {
            is_featured = 1;
        } else {
            is_featured = 0;
        }

        wpestate_listing_pay(prop_id, $(this), is_featured, is_upgrade);
    });
 
    jQuery('.woo_pay_submit').on('click', function () {
        var pay_paypal, prop_id, book_id, invoice_id, is_featured, is_upgrade, ajaxurl, depozit, is_submit, pack_id;
        prop_id = jQuery(this).attr('data-propid');
        depozit = jQuery(this).attr('data-deposit');
        is_featured = jQuery(this).attr('data-is_featured');
        pack_id = jQuery('.package_selected').attr('data-id');
        is_upgrade = 0;
        is_submit = 1;
        if (jQuery('#open_packages').length > 0) {
            is_submit = 0;
        }
        book_id = '';
        invoice_id = '';

        ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
        jQuery.ajax({
            type: 'POST',
            url: ajaxurl,
            data: {
                'action': 'wpestate_woo_pay',
                'propid': prop_id,
                'depozit': depozit,
                'is_submit': is_submit,
                'is_featured': is_featured,
                'pack_id': pack_id,
                'book_id': book_id,
                'invoiceid': invoice_id,
//                    'price_pack'    :   price_pack,
//                    'security'      :   nonce,
            },
            success: function (data) {
                if (data !== false) {
                    window.location.href = ajaxcalls_vars.checkout_url;
                }
            },
            error: function (errorThrown) {}
        });//end ajax
    });

    $('.listing_upgrade').on('click', function (event) {
        var is_upgrade, is_featured, prop_id;
        is_upgrade = 1;
        is_featured = 0;
        prop_id = $(this).attr('data-listingid');
        wpestate_listing_pay(prop_id, $(this), is_featured, is_upgrade);
    });



    ///////////////////////////////////////////////////////////////////////////////////////////
    /////// Property page  + ajax call on contact
    ///////////////////////////////////////////////////////////////////////////////////////////


  

    ///////////////////////////////////////////////////////////////////////////////////////////
    ////////  property listing listing
    ////////////////////////////////////////////////////////////////////////////////////////////

    $('.listing_filters_head li').on('click', function (event) {

        var pick, value, parent;
        pick = $(this).text();
        value = $(this).attr('data-value');
        parent = $(this).parent().parent();
        parent.find('.filter_menu_trigger').text(pick).append('<span class="caret caret_filter"></span>').attr('data-value', value);
        parent.find('input:hidden').val(value);

        wpestate_start_filtering(1);


    });




    ///////////////////////////////////////////////////////////////////////////////////////////
    //////// advanced search filtering
    ////////////////////////////////////////////////////////////////////////////////////////////

       $('.wpresidence_property_list_half_map_core .wpresidence_wrap_a_filter_order li').on('click', function (event) {
        console.log('wpresidence_half_map_filter_wrapper ');
        var pick, value, parent;
        pick = $(this).text();
        value = $(this).attr('data-value');
        parent = $(this).parent().parent();
        parent.find('.filter_menu_trigger').text(pick).append('<span class="caret caret_filter"></span>').attr('data-value', value);
        parent.find('input:hidden').val(value);

        if(mapfunctions_vars.custom_search==='yes'){
            jQuery('.half-pagination').remove();

                    console.log('dddddddddddddddddd ');
            wpestate_custom_search_start_filtering_ajax(1);
        }else{

            
                    console.log('lllllllllllll ');
            wpestate_start_filtering_ajax(1);
        }

    });


    $('.adv_listing_filters_head li').on('click', function (event) {

        var pick, value, parent, args, page_id, ajaxurl;
        ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
        pick = $(this).text();
        value = $(this).attr('data-value');
        parent = $(this).parent().parent();
        parent.find('.filter_menu_trigger').text(pick).append('<span class="caret caret_filter"></span>').attr('data-value', value);
        args = $('#searcharg').val();
        page_id = $('#page_idx').val();
        //$('#listing_ajax_container').empty();
        //$('#listing_loader').show();


        const listingContainer = jQuery('#listing_ajax_container');
        wpestate_createSkeletons(listingContainer);


        var nonce = jQuery('#wpestate_search_nonce').val();
        $.ajax({
            type: 'POST',
            url: ajaxurl,

            data: {
                'action': 'wpestate_advanced_search_filters',
                'args': args,
                'value': value,
                'page_id': page_id,
                'security': nonce
            },
            success: function (data) {
                //$('#listing_loader').hide();
                //$('#listing_ajax_container').append(data);
                 wpestate_replaceSkeletons(listingContainer, data);
                
                
                wpestate_restart_js_after_ajax();
                wpestate_add_pagination_orderby();
            },
            error: function (errorThrown) {
            }
        }); //end ajax
    });


    function wpestate_add_pagination_orderby() {

        var order = $('#a_filter_order').attr('data-value');
        jQuery('.pagination a').each(function () {
            var href = $(this).attr('href');
            href = href + "&order_search=" + order;
            $(this).attr('href', href);
        });
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    ////////  Ajax add to favorites on listing
    ////////////////////////////////////////////////////////////////////////////////////////////
    $('.icon-fav,.remove_fav_dash').on('click', function (event) {
        event.stopPropagation();
        var icon = $(this);
        wpestate_add_remove_favorite(icon);
    });




    // remove from fav listing on user profile
    $('.icon-fav-on-remove').on('click', function (event) {
        event.stopPropagation();
        $(this).parent().parent().remove();

    });





    ///////////////////////////////////////////////////////////////////////////////////////////
    ////////  update profile
    ////////////////////////////////////////////////////////////////////////////////////////////
    $('#register_agent').on('click', function (event) {
        var firstname = $('#firstname').val();
        var secondname = $('#secondname').val();
        var useremail = $('#useremail').val();
        var userphone = $('#userphone').val();
        var usermobile = $('#usermobile').val();
        var userskype = $('#userskype').val();
        var usertitle = $('#usertitle').val();
        var description = $('#about_me').val();
        var userfacebook = $('#userfacebook').val();
        var usertwitter = $('#usertwitter').val();
        var userlinkedin = $('#userlinkedin').val();
        var userpinterest = $('#userpinterest').val();
        var userinstagram = $('#userinstagram').val();
        var userurl = $('#website').val();
        var agent_username = $('#agent_username').val();
        var agent_password = $('#agent_password').val();
        var agent_repassword = $('#agent_repassword').val();
        var agent_category_submit = $('#agent_category_submit').val();
        var agent_action_submit = $('#agent_action_submit').val();
        var agent_city = $('#agent_city').val();
        var agent_county = $('#agent_county').val();
        var agent_member = $('#agent_member').val();
        var agent_address = $('#agent_address').val();

        var agent_youtube = $('#useryoutube').val();
        var agent_tiktok = $('#usertiktok').val();
        var agent_telegram = $('#usertelegram').val();
        var agent_vimeo = $('#uservimeo').val();
        var agent_private_notes = $('#agent_private_notes').val();

        var agent_area = $('#agent_area').val();
        var is_agent_edit = $('#is_agent_edit').val();
        var user_id = $('#user_id').val();
        var agent_id = $('#agent_id').val();
        var ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
        var securityprofile = $('#security-profile').val();
        var upload_picture = $('#upload_picture').val();
        var profile_image_url = $('#profile-image').attr('data-profileurl');
        var profile_image_url_small = $('#profile-image').attr('data-smallprofileurl');
        var acf_fields = wpestate_return_form_acf_fields('.field-group');

        // customparameters
        var agent_custom_label = [];
        $('.single_parameter_row .agent_custom_label').each(function () {
            agent_custom_label.push($(this).val());
        });

        var agent_custom_value = [];
        $('.single_parameter_row .agent_custom_value').each(function () {
            agent_custom_value.push($(this).val());
        });
        var nonce = jQuery('#wpestate_register_agent_nonce').val();

        window.scrollTo(0, 0);

        $('#profile_message').empty().append('<div class="login-alert">' + ajaxcalls_vars.saving + '<div>');


        $.ajax({
            type: 'POST',
            url: ajaxurl,
            dataType: 'json',
            data: {
                'action': 'wpestate_ajax_register_agent',
                'agent_username': agent_username,
                'agent_password': agent_password,
                'agent_repassword': agent_repassword,
                'firstname': firstname,
                'secondname': secondname,
                'useremail': useremail,
                'userphone': userphone,
                'usermobile': usermobile,
                'userskype': userskype,
                'usertitle': usertitle,
                'description': description,
                'upload_picture': upload_picture,
                'security-profile': securityprofile,
                'profile_image_url': profile_image_url,
                'profile_image_url_small': profile_image_url_small,
                'userfacebook': userfacebook,
                'usertwitter': usertwitter,
                'userlinkedin': userlinkedin,
                'userpinterest': userpinterest,
                'userinstagram': userinstagram,
                'userurl': userurl,
                'agent_category_submit': agent_category_submit,
                'agent_action_submit': agent_action_submit,
                'agent_city': agent_city,
                'agent_county': agent_county,
                'agent_area': agent_area,
                'agentedit': is_agent_edit,
                'userid': user_id,
                'agentid': agent_id,
                'agent_member': agent_member,
                'agent_address': agent_address,
                'agent_youtube':agent_youtube,
                'agent_tiktok':agent_tiktok,
                'agent_telegram':agent_telegram,
                'agent_vimeo': agent_vimeo,
                'agent_private_notes': agent_private_notes,
                'acf_fields': JSON.stringify(acf_fields),
                'agent_custom_label': agent_custom_label,
                'agent_custom_value': agent_custom_value,
                'security': nonce

            },
            success: function (data) {
                $('#profile_message').empty().append('<div class="login-alert">' + data.mesaj + '<div>');
                if (data.added) {
                    setTimeout(function () {
                        window.open(ajaxcalls_vars.agent_list, '_self', false);
                    }, 1500);
                }
            },
            error: function (errorThrown) {

            }
        });
    });


    $('#update_profile').on('click', function (event) {

        $('#profile_message').find('.login-alert').remove();

        var firstname, secondname, userurl, usermobile, userinstagram, userpinterest, userlinkedin, usertwitter, userfacebook, profile_image_url, profile_image_url_small, firstname, secondname, useremail, userphone, userskype, usertitle, description, ajaxurl, securityprofile, upload_picture;
        firstname = $('#firstname').val();
        secondname = $('#secondname').val();
        useremail = $('#useremail').val();
        userphone = $('#userphone').val();
        usermobile = $('#usermobile').val();
        userskype = $('#userskype').val();
        usertitle = $('#usertitle').val();
        description = $('#about_me').val();
        userfacebook = $('#userfacebook').val();
        usertwitter = $('#usertwitter').val();
        userlinkedin = $('#userlinkedin').val();
        userpinterest = $('#userpinterest').val();
        userinstagram = $('#userinstagram').val();
        userurl = $('#website').val();
        var hubspot_api = $('#hubspot_api').val();
        var agent_member = $('#agent_member').val();
        var agent_address= $('#agent_address').val();

        var agent_youtube = $('#useryoutube').val();
        var agent_tiktok = $('#usertiktok').val();
        var agent_telegram = $('#usertelegram').val();
        var agent_vimeo = $('#uservimeo').val();
        var agent_private_notes = $('#agent_private_notes').val();

        var agent_category_submit = $('#agent_category_submit').val();
        var agent_action_submit = $('#agent_action_submit').val();
        var agent_city = $('#agent_city').val();
        var agent_county = $('#agent_county').val();
        var agent_area = $('#agent_area').val();
        var nonce = jQuery('#wpestate_update_profile_nonce').val();

        ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
        securityprofile = $('#security-profile').val();
        upload_picture = $('#upload_picture').val();
        profile_image_url = $('#profile-image').attr('data-profileurl');
        profile_image_url_small = $('#profile-image').attr('data-smallprofileurl');

        var acf_fields = wpestate_return_form_acf_fields('.field-group');

        // customparameters
        var agent_custom_label = [];
        $('.cliche_row .agent_custom_label').each(function () {
            agent_custom_label.push($(this).val());
        });

        var agent_custom_value = [];
        $('.cliche_row .agent_custom_value').each(function () {
            agent_custom_value.push($(this).val());
        });



        window.scrollTo(0, 0);
        $('#profile_message').empty().append('<div class="login-alert">' + ajaxcalls_vars.saving + '<div>');
        $.ajax({
            type: 'POST',
            url: ajaxurl,
            data: {
                'action': 'wpestate_ajax_update_profile',
                'firstname': firstname,
                'secondname': secondname,
                'useremail': useremail,
                'userphone': userphone,
                'usermobile': usermobile,
                'userskype': userskype,
                'usertitle': usertitle,
                'description': description,
                'upload_picture': upload_picture,
                'security-profile': securityprofile,
                'profile_image_url': profile_image_url,
                'profile_image_url_small': profile_image_url_small,
                'userfacebook': userfacebook,
                'usertwitter': usertwitter,
                'userlinkedin': userlinkedin,
                'userpinterest': userpinterest,
                'userinstagram': userinstagram,
                'userurl': userurl,
                'agent_category_submit': agent_category_submit,
                'agent_action_submit': agent_action_submit,
                'agent_city': agent_city,
                'agent_county': agent_county,
                'agent_area': agent_area,
                'agent_member': agent_member,
                'agent_address':agent_address,
                'agent_youtube':agent_youtube,
                'agent_tiktok':agent_tiktok,
                'agent_telegram':agent_telegram,
                'agent_vimeo': agent_vimeo,
                'agent_private_notes': agent_private_notes,
                'agent_custom_label': agent_custom_label,
                'agent_custom_value': agent_custom_value,
                'acf_fields': JSON.stringify(acf_fields),
                'hubspot_api': hubspot_api,
                'security': nonce,

            },
            success: function (data) {

                $('#profile_message').empty().append('<div class="login-alert">' + data + '<div>');
                window.scrollTo(0, 0);
            },
            error: function (errorThrown) {
            }
        });
    });



    $('#update_profile_agency').on('click', function () {

        var hubspot_api, agency_opening_hours, agency_license, agency_long, agency_lat, agency_address, agency_area, agency_county, agency_city, agency_action_submit, agency_action_submit, agency_category_submit, agency_taxes, agency_website, agency_languages, agency_name, userurl, usermobile, userinstagram, userpinterest, userlinkedin, usertwitter, userfacebook, profile_image_url, profile_image_url_small, firstname, secondname, useremail, userphone, userskype, usertitle, description, ajaxurl, securityprofile, upload_picture;
        agency_name = $('#agency_title').val();
        useremail = $('#useremail').val();
        userphone = $('#userphone').val();
        usermobile = $('#usermobile').val();
        userskype = $('#userskype').val();

        description = $('#about_me').val();
        userfacebook = $('#userfacebook').val();
        usertwitter = $('#usertwitter').val();
        userlinkedin = $('#userlinkedin').val();
        userpinterest = $('#userpinterest').val();
        userinstagram = $('#userinstagram').val();

        var useryoutube = $('#useryoutube').val();
        var usertiktok = $('#usertiktok').val();
        var usertelegram = $('#usertelegram').val();
        var uservimeo = $('#uservimeo').val();
        var agency_private_notes = $('#agency_private_notes').val();



        agency_languages = $('#agency_languages').val();
        agency_website = $('#agency_website').val();
        agency_taxes = $('#agency_taxes').val();
        agency_license = $('#agency_license').val();


        agency_category_submit = $('#agency_category_submit').val();
        agency_action_submit = $('#agency_action_submit').val();
        agency_city = $('#agency_city').val();
        agency_county = $('#agency_county').val();
        agency_area = $('#agency_area').val();
        agency_address = $('#agency_address').val();
        agency_lat = $('#agency_lat').val();
        agency_long = $('#agency_long').val();
        agency_opening_hours = $('#agency_opening_hours ').val();
        hubspot_api = $('#hubspot_api ').val();
        securityprofile = $('#security-profile').val();
        upload_picture = $('#upload_picture').val();
        profile_image_url = $('#profile-image').attr('data-profileurl');
        profile_image_url_small = $('#profile-image').attr('data-smallprofileurl');

        var acf_fields = wpestate_return_form_acf_fields('.field-group');

        ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';

        var nonce = jQuery('#wpestate_update_profile_nonce').val();
        $('#profile_message').empty().append('<div class="login-alert">' + ajaxcalls_vars.saving + '<div>');
        window.scrollTo(0, 0);
        $.ajax({
            type: 'POST',
            url: ajaxurl,
            data: {
                'action': 'wpestate_ajax_update_profile_agency',
                'agency_name': agency_name,
                'useremail': useremail,
                'userphone': userphone,
                'usermobile': usermobile,
                'userskype': userskype,
                'usertitle': usertitle,
                'description': description,
                'upload_picture': upload_picture,
                'security-profile': securityprofile,
                'profile_image_url': profile_image_url,
                'profile_image_url_small': profile_image_url_small,
                'userfacebook': userfacebook,
                'usertwitter': usertwitter,
                'userlinkedin': userlinkedin,
                'userpinterest': userpinterest,
                'userinstagram': userinstagram,
                'useryoutube':useryoutube,
                'usertiktok':usertiktok,
                'usertelegram':usertelegram,
                'uservimeo':uservimeo,
                'agency_private_notes':agency_private_notes,
                'userurl': userurl,
                'agency_languages': agency_languages,
                'agency_website': agency_website,
                'agency_taxes': agency_taxes,
                'agency_license': agency_license,
                'agency_category_submit': agency_category_submit,
                'agency_action_submit': agency_action_submit,
                'agency_city': agency_city,
                'agency_county': agency_county,
                'agency_area': agency_area,
                'agency_address': agency_address,
                'agency_lat': agency_lat,
                'agency_opening_hours': agency_opening_hours,
                'agency_long': agency_long,
                'hubspot_api': hubspot_api,
                'acf_fields': JSON.stringify(acf_fields),
                'security': nonce
            },
            success: function (data) {
                $('#profile_message').empty().append('<div class="login-alert">' + data + '<div>');
                window.scrollTo(0, 0);
            },
            error: function (errorThrown) {
                console.log(errorThrown);
            }
        });
    });



//    update developer profile
    $('#update_profile_developer').on('click', function () {

        var hubspot_api, developer_license, developer_long, developer_lat, developer_address, developer_area, developer_county, developer_city, developer_action_submit, developer_action_submit, developer_category_submit, developer_taxes, developer_website, developer_languages, developer_name, userurl, usermobile, userinstagram, userpinterest, userlinkedin, usertwitter, userfacebook, profile_image_url, profile_image_url_small, firstname, secondname, useremail, userphone, userskype, usertitle, description, ajaxurl, securityprofile, upload_picture;
        developer_name = $('#developer_title').val();
        useremail = $('#useremail').val();
        userphone = $('#userphone').val();
        usermobile = $('#usermobile').val();
        userskype = $('#userskype').val();

        description = $('#about_me').val();
        userfacebook = $('#userfacebook').val();
        usertwitter = $('#usertwitter').val();
        userlinkedin = $('#userlinkedin').val();
        userpinterest = $('#userpinterest').val();
        userinstagram = $('#userinstagram').val();


       var useryoutube = $('#useryoutube').val();
       var usertiktok = $('#usertiktok').val();
       var usertelegram = $('#usertelegram').val();
       var  uservimeo = $('#uservimeo').val();
       var  developer_private_notes = $('#developer_private_notes').val();



        developer_languages = $('#developer_languages').val();
        developer_website = $('#developer_website').val();
        developer_taxes = $('#developer_taxes').val();
        developer_license = $('#developer_license').val();


        developer_category_submit = $('#developer_category_submit').val();
        developer_action_submit = $('#developer_action_submit').val();
        developer_city = $('#developer_city').val();
        developer_county = $('#developer_county').val();
        developer_area = $('#developer_area').val();
        developer_address = $('#developer_address').val();
        developer_lat = $('#developer_lat').val();
        developer_long = $('#developer_long').val();
        securityprofile = $('#security-profile').val();
        upload_picture = $('#upload_picture').val();
        profile_image_url = $('#profile-image').attr('data-profileurl');
        profile_image_url_small = $('#profile-image').attr('data-smallprofileurl');
        hubspot_api = $('#hubspot_api').val();

        var acf_fields = wpestate_return_form_acf_fields('.field-group');

        ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
        var nonce = jQuery('#wpestate_update_profile_nonce').val();
        $('#profile_message').empty().append('<div class="login-alert">' + ajaxcalls_vars.saving + '<div>');
        window.scrollTo(0, 0);
        $.ajax({
            type: 'POST',
            url: ajaxurl,
            data: {
                'action': 'wpestate_ajax_update_profile_developer',
                'developer_name': developer_name,
                'useremail': useremail,
                'userphone': userphone,
                'usermobile': usermobile,
                'userskype': userskype,
                'usertitle': usertitle,
                'description': description,
                'upload_picture': upload_picture,
                'security-profile': securityprofile,
                'profile_image_url': profile_image_url,
                'profile_image_url_small': profile_image_url_small,
                'userfacebook': userfacebook,
                'usertwitter': usertwitter,
                'userlinkedin': userlinkedin,
                'userpinterest': userpinterest,
                'userinstagram': userinstagram,
                'userurl': userurl,
                'developer_languages': developer_languages,
                'developer_website': developer_website,
                'developer_taxes': developer_taxes,
                'developer_license': developer_license,
                'developer_category_submit': developer_category_submit,
                'developer_action_submit': developer_action_submit,
                'developer_city': developer_city,
                'developer_county': developer_county,
                'developer_area': developer_area,
                'developer_address': developer_address,
                'developer_lat': developer_lat,
                'developer_long': developer_long,
                'hubspot_api': hubspot_api,
                'useryoutube':useryoutube,
                'usertiktok':usertiktok,
                'usertelegram':usertelegram,
                'uservimeo':uservimeo,
                'developer_private_notes':developer_private_notes,
                'acf_fields': JSON.stringify(acf_fields),
                'security': nonce
            },
            success: function (data) {
                $('#profile_message').empty().append('<div class="login-alert">' + data + '<div>');
                window.scrollTo(0, 0);
            },
            error: function (errorThrown) {
            }
        });
    });

    //delete profile

    $('#delete_profile').on('click', function (event) {
        var ajaxurl;
        ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
        var nonce = jQuery('#wpestate_update_profile_nonce').val();
        var result = confirm(ajaxcalls_vars.delete_account);
        if (result) {
            //Logic to delete the item

            $.ajax({
                type: 'POST',
                url: ajaxurl,
                data: {
                    'action': 'wpestate_delete_profile',
                    'security': nonce,
                },
                success: function (data) {
                    window.location = '/';
                },
                error: function (errorThrown) {

                }
            });
        }
    });

    //end delete profile

}); // end ready jquery
//End ready ********************************************************************


/**
 * Handles the loading of agency/developer listings via AJAX.
 * This function is triggered when a user clicks on a term bar item.
 */
function wpestate_load_agency_developer_listings() {
    jQuery('.term_bar_item, .listing_load_more').on('click', function (event) {
       
       
        // Select relevant DOM elements
        var $this = jQuery(this);
        var $listingParent = $this.parents('.single_listing_block');
        var listings_per_page= $listingParent.attr('data-listings_per_page');
        var rownumber= $listingParent.attr('data-rownumber');

        var $loadMoreButton = jQuery('.load_more_ajax_cont .listing_load_more', $listingParent);
        var $agencyListingsWrapper = jQuery('.agency_listings_wrapper');
        var $listingLoader = jQuery('#listing_loader');
        var $termBarWrapper = jQuery('.term_bar_wrapper');
        var $areadyLoadedItems = jQuery('.listing_wrapper', $listingParent).length;
     
        var $page_with_sidebar = 0;
        if(jQuery('#primary_sidebar_wrapper').length > 0){
            $page_with_sidebar = 1;
        }

        // Display load more button
        $loadMoreButton.fadeIn();


        // Perform AJAX request
        if(!$this.hasClass('listing_load_more')){
            //$agencyListingsWrapper.empty();
            $areadyLoadedItems=0;

            // Update active term styling
            jQuery('.term_bar_item').removeClass('active_term');
            $this.addClass('active_term');
         
        }


        
        
        var $active_tab = jQuery('.active_term', $listingParent);
        var term_name = jQuery($active_tab).attr('data-term_name');


       
        // Prepare AJAX data
        var ajaxData = {
            action: 'wpestate_agency_listings',
            listings_per_page:listings_per_page,
            rownumber:rownumber,
            term_name: $this.attr('data-term_name'),
            agency_id: $termBarWrapper.attr('data-agency_id'),
            agent_id: $termBarWrapper.attr('data-agent_id'),
            post_id: $termBarWrapper.attr('data-post_id'),
            term_name: term_name,
            loaded: $areadyLoadedItems,
            is_agency: jQuery('.single-estate_agency').length > 0 ? 1 : 0,
            page_with_sidebar: $page_with_sidebar,
            security: jQuery('#wpestate_developer_listing_nonce').val()
        };

     



        const listingContainer = jQuery('.agency_listings_wrapper');
        
        if(!$this.hasClass('listing_load_more')){
            wpestate_createSkeletons(listingContainer);
        }else{
             wpestate_createSkeletons(listingContainer,true);
        }

       // $listingLoader.show();

        jQuery.ajax({
            type: 'POST',
            url: ajaxcalls_vars.admin_url + 'admin-ajax.php',
            data: ajaxData,
            dataType: 'json',
            success: function (response) {
                $listingLoader.hide();
                if (response.success) {
                    
                    if(response.data.found_posts > 0){
                        if(!$this.hasClass('listing_load_more')){
                            //$agencyListingsWrapper.html(response.data.html);
                            wpestate_replaceSkeletons(listingContainer, response.data.html);
   
                        }else{
             
                            //$agencyListingsWrapper.append(response.data.html);
                            wpestate_replaceSkeletons(listingContainer,response.data.html);

                        }
              
                        wpestate_restart_js_after_ajax();
                    }else{
                        //$agencyListingsWrapper.append(response.data.html);
                        wpestate_replaceSkeletons(listingContainer,response.data.html);
                        if($this.hasClass('listing_load_more')){
                            $this.hide();
                        }
                    }



                } else {
                    $agencyListingsWrapper.html('<p>' + response.data.message + '</p>');
                }
            },
            error: function (jqXHR, textStatus, errorThrown) {
                console.error('AJAX request failed:', textStatus, errorThrown);
                $listingLoader.hide();
                $agencyListingsWrapper.html('<p>Error loading listings. Please try again.</p>');
            }
        });
    });
}
 

function wpestate_add_to_favorites() {


    jQuery('#add_favorites').on('click', function (event) {

        var post_id, ajaxurl;
        post_id = jQuery('#add_favorites').attr('data-postid');
        var icon = jQuery('#add_favorites');
        ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
        var nonce = jQuery('#wpestate_ajax_filtering').val();
        var use_favorite_login  =   ajaxcalls_vars.favorites_login;
        if(use_favorite_login=="no"){
            wpestate_add_remove_favorite_local_storage_single(icon,post_id);
            return;
        }



        if (parseInt(ajaxcalls_vars.userid, 10) === 0) {
            if (!Modernizr.mq('only all and (max-width: 768px)')) {
                jQuery('#modal_login_wrapper').show();
                 jQuery('#modal_login_wrapper').find('[autofocus]').focus();
                jQuery('.loginpop').val('1');
            } else {
                jQuery('.mobile-trigger-user').trigger('click');
            }

        } else {
            jQuery('#add_favorites').text(ajaxcalls_vars.saving);
            jQuery.ajax({
                type: 'POST',
                url: ajaxurl,
                dataType: 'json',
                data: {
                    'action': 'wpestate_ajax_add_fav',
                    'post_id': post_id,
                    'security': nonce
                },
                success: function (data) {
                    if (data.added) {
                        jQuery('#add_favorites').removeClass('isnotfavorite').addClass('isfavorite').attr('title', ajaxcalls_vars.remove_favorite);
                        jQuery('#add_favorites').html('<i class="fas fa-heart"></i>' + ajaxcalls_vars.favorite);

                        // Refresh the Bootstrap tooltip if it exists
                        if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
                            var tooltipInstance = bootstrap.Tooltip.getInstance(icon[0]);
                            if (tooltipInstance) {
                                tooltipInstance.setContent({
                                    '.tooltip-inner': ajaxcalls_vars.remove_fav
                                });
                            }
                        }


                    } else {
                        jQuery('#add_favorites').removeClass('isfavorite').addClass('isnotfavorite').attr('title', ajaxcalls_vars.add_favorite);
                        jQuery('#add_favorites').html('<i class="far fa-heart"></i>' + ajaxcalls_vars.favorite);

                        // Refresh the Bootstrap tooltip if it exists
                        if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
                            var tooltipInstance = bootstrap.Tooltip.getInstance(icon[0]);
                            if (tooltipInstance) {
                                tooltipInstance.setContent({
                                    '.tooltip-inner': ajaxcalls_vars.remove_fav
                                });
                            }
                        }

                    }
                },
                error: function (errorThrown) {
                }
            }); //end ajax
        }// end check login
    });
}

///////////////////////////////////////////////////////////////////////////////////////////
/////// Property page  + ajax call on contact
///////////////////////////////////////////////////////////////////////////////////////////
function wpestate_theme_slider_show_contact() {
    jQuery('.wpestate_theme_slider_contact_agent').on('click', function () {
        var acesta = jQuery(this).parent().parent();
        acesta.find('#show_contact').text(ajaxcalls_vars.contact_agent);
        acesta.find('.theme_slider_contact_form_wrapper').show('0').toggleClass('theme_slider_contact_form_wrapper_visible');
        jQuery('#estate-carousel').carousel('pause');
      
    });



    jQuery('.theme_slider_details_modal_close').on('click', function () {
        jQuery(this).parent().removeClass('theme_slider_contact_form_wrapper_visible');
                jQuery('#estate-carousel').carousel('cycle');
    });

    jQuery('.theme_slider_contact_form_wrapper').on('click', function (event) {
        event.stopPropagation();
    });


}


/**
 * Enable Schedule Contact Functionality
 *
 * This function sets up event handlers for the schedule meeting functionality
 * and initializes the datepicker for schedule day selection.
 *
 * @package WpResidence
 * @subpackage PropertyDetails
 * @since 3.0.3
 *
 * Dependencies:
 * - jQuery
 * - jQuery UI Datepicker
 *
 * Usage: Call this function after DOM is ready and dependencies are loaded.
 */

function wpestate_enable_schedule_contact() {
    // Toggle schedule wrapper visibility when clicking on schedule meeting button
    jQuery('.schedule_meeting').on('click', function (event) {
        var parent = jQuery(this).parent().parent();
        var scheduleWrapper = parent.find('.schedule_wrapper');
       
        scheduleWrapper.slideToggle(400, function() {
            scheduleWrapper.css('display', scheduleWrapper.is(':visible') ? 'flex' : 'none');
        });
    });
    

    
    // Initialize datepicker for schedule day input with position correction
    jQuery(".schedule_day, #schedule_day").datepicker({
        dateFormat: "yy-mm-dd",
        beforeShow: function(input, inst) {
            // Position correction
            setTimeout(function() {
                inst.dpDiv.css({
                    position: 'absolute',
                    top: jQuery(input).offset().top + jQuery(input).outerHeight(),
                    left: jQuery(input).offset().left
                });
            }, 0);
        }
    }).datepicker('widget').wrap('<div class="ll-skin-melon"/>');
}


function wpestate_agent_submit_email() {
    jQuery('.wpresidence_elementor_form').on('submit', function (event) {
        event.preventDefault();
        var form_submit = jQuery('.wpresidence_elementor_form').find('.agent_submit_class_elementor');
        process_form_submit(form_submit);
    });

    jQuery('.agent_submit_class').on('click', function (event) {
        process_form_submit(jQuery(this));
    });
    
    jQuery('.message_submit').on('click', function (event) {
        process_form_submit(jQuery(this));
    });
}


function process_form_submit(form_submit) {

    var parent, contact_name, contact_email, contact_phone, contact_coment, agent_id, property_id, nonce, ajaxurl;
    parent          = form_submit.closest('.wpestate_contact_form_parent');

    var is_private_message='no';
    if(form_submit.hasClass('message_submit')){
        is_private_message='yes';
    }
    

    contact_name    = parent.find('#agent_contact_name').val();
    contact_email   = parent.find('#agent_user_email').val();
    contact_phone   = parent.find('#agent_phone').val();
    contact_coment  = parent.find('#agent_comment').val();
    agent_id        = parent.find('#agent_id').val();
    property_id     = parent.find('#agent_property_id').val();
    
    nonce   = parent.find('#agent_property_ajax_nonce').val();
    ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
    
    var schedule_day = parent.find('.schedule_day').val();
    var schedule_hour = parent.find('.schedule_hour').val();
    var schedule_mode ='in_person';


     
    if( parent.hasClass('wpestate_schedule_tour_wrapper_content') || 
        parent.hasClass('wpestate_shedule_tour_wrapper_type2') ||
        parent.hasClass('wpestate_schedule_tour_wrapper') ){

        schedule_day    =   parent.find('.shedule_day_option_selected').attr('data-scheudle-day');
        schedule_hour   =   parent.find('#wpestate_schedule_tour_time').val();

        var schedule_mode_button   =   parent.find('.shedule_option_selected');
        if( schedule_mode_button.hasClass('schedule_video_chat') ){
            schedule_mode ='video_chat';
        }       
    }


    var is_elementor = 0;
    if (form_submit.parents('.wpresidence_elementor_form').length) {
        parent                  =   form_submit.parents('.wpresidence_elementor_form');
        contact_name            =   parent.find('#form-field-name').val();
        contact_email           =   parent.find('#form-field-email').val();
        contact_coment          =   parent.find('#form-field-message').val();
        contact_phone           =   parent.find('input[name="phone"]').val();
        nonce                   =   parent.find('#agent_property_ajax_nonce').val();
        is_elementor            =   parent.find('#contact_form_elementor').val();
        var wpresidence_form_id =   parent.find('#wpresidence_form_id').val();
        var elementor_email_subject = parent.find('#elementor_email_subject').val();

        var temp_details;
        temp_details        = '';
        var elementor_form  = form_submit.parents('.wpresidence_elementor_form');
        var form_items      = elementor_form.find('.elementor-field');
        var label_name      ='';
        var labelElement;
        form_items.each(function () {

            labelElement=jQuery(this).parent().find('.elementor-field-label');
            
            if (labelElement.length > 0) {
                label_name = labelElement.text();
                label_name = label_name.replace('*', '');
            } else {
                label_name = jQuery(this).attr('name')
            }
  
            temp_details = temp_details + label_name + ": " + jQuery(this).val() + "/n";
        });

        contact_coment = temp_details ;

    }

    if (parent.find('.wpestate_agree_gdpr').length > 0) {
        if (!parent.find('.wpestate_agree_gdpr').is(':checked')) {
            parent.find('#alert-agent-contact').empty().append(ajaxcalls_vars.gdpr_terms);
            return;
        }
    }



    parent.find('#alert-agent-contact').empty().append(ajaxcalls_vars.sending);

    jQuery.ajax({
        type: 'POST',
        dataType: 'json',
        url: ajaxurl,
        data: {
            'action': 'wpestate_ajax_agent_contact_form',
            'name': contact_name,
            'email': contact_email,
            'phone': contact_phone,
            'comment': contact_coment,
            'agent_id': agent_id,
            'propid': property_id,
            'schedule_day': schedule_day,
            'schedule_hour': schedule_hour,
            'schedule_mode':schedule_mode,
            'elementor_wpresidence_form_id': wpresidence_form_id,
            'elementor_email_subject': elementor_email_subject,
            'is_elementor': is_elementor,
            'is_private_message':is_private_message,
            'nonce': nonce
        },
        success: function (data) {

            if (data.sent) {
                parent.find('#agent_contact_name').val('');
                parent.find('#agent_user_email').val('');
                parent.find('#agent_phone').val('');
                parent.find('#agent_comment').val('');
                parent.find('.schedule_day').val('');
                parent.find('#schedule_hour').val('');
                parent.find('#alert-agent-contact').addClass(' wpestate-agent-contact-sent ').empty().append(data.response);


                if (typeof (form_items) !== 'undefined') {
                    form_items.each(function () {
                        jQuery(this).val('');
                    });
                }

            } else {
                parent.find('#alert-agent-contact').empty().append(data.response);
            }

        },
        error: function (errorThrown) {

        }
    });



    jQuery.ajax({
        type: 'POST',
        //  dataType: 'json',
        url: ajaxurl,
        data: {
            'action': 'wpestate_ajax_agent_contact_form_forcrm',
            'name': contact_name,
            'email': contact_email,
            'phone': contact_phone,
            'comment': contact_coment,
            'agent_id': agent_id,
            'propid': property_id,
            'schedule_day': schedule_day,
            'schedule_hour': schedule_hour,
            'schedule_mode':schedule_mode,
            'nonce': nonce
        },
        success: function (data) {
        },
        error: function (errorThrown) {

        }
    });


}


///////////////////////////////////////////////////////////////////////////////////////////
/////// Property page  + ajax call on contact
///////////////////////////////////////////////////////////////////////////////////////////


function wpestate_enable_share_unit(){
   
    jQuery('.share_list').unbind('click');
    jQuery('.share_list').on( 'click', function(event) {
        event.stopPropagation();   
        var sharediv=jQuery(this).parent().find('.share_unit');
        sharediv.toggle();
        jQuery(this).toggleClass('share_on');
    });
    
}

function setCookie(cname, cvalue, exdays) {
    const d = new Date();
    d.setTime(d.getTime() + (exdays * 24 * 60 * 60 * 1000));
    let expires = "expires="+d.toUTCString();
    document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
  }


function wpestate_enable_not_logged_favorites(){
   
    var use_favorite_login  =   ajaxcalls_vars.favorites_login;

    if(use_favorite_login=="yes"){
        return;
    }

    var current_favorites = window.localStorage.getItem('wpestate_favorites');
    setCookie('wpestate_favorites',current_favorites,10);
    var prop_ids='';

    if( current_favorites!==null ){
        prop_ids = jQuery.map(current_favorites.split(','), function(value){
            return parseInt(value);
        });
    }
  
    jQuery('.icon-fav').each(function(){
        var icon=jQuery(this);
        var property_id=icon.attr('data-postid');
        property_id=parseInt(property_id);
        var index = prop_ids.indexOf( property_id );    

        if (index > -1) {
            icon.removeClass('icon-fav-off').addClass('icon-fav-on');
            icon.attr('title', ajaxcalls_vars.remove_fav);
    
        }else{
            icon.removeClass('icon-fav-on').addClass('icon-fav-off');
            icon.attr('title', ajaxcalls_vars.add_favorite);
    
        }
    })
    

    if(jQuery('#add_favorites').length>0){
        var property_id=jQuery('#add_favorites').attr('data-postid');
        property_id=parseInt(property_id);
        var index = prop_ids.indexOf( property_id );    
       
        if (index > -1) {
            jQuery('#add_favorites').removeClass('isnotfavorite').addClass('isfavorite').attr('title', ajaxcalls_vars.remove_favorite);
            jQuery('#add_favorites').html('<i class="fas fa-heart"></i>' + ajaxcalls_vars.favorite);
    
        }  
    }
    
}

/**
 * Load Agent Listings
 * 
 * This function handles the loading of agent listings when a term bar item is clicked.
 * It updates the active term, fetches new listings via AJAX, and updates the display.
 *
 * @since 1.0.0
 * @package WPResidence
 * @subpackage AgentListings
 */
function wpestate_loade_agent_listings() {
    jQuery('.term_bar_item').on('click', function (event) {
    

        // Cache frequently used selectors
        var $this = jQuery(this);
        var $listingParent = $this.closest('.single_listing_block');
        var $agencyListingsWrapper = jQuery('.agency_listings_wrapper');
        var $listingLoader = jQuery('#listing_loader');

        // Update UI to show loading state
        $listingParent.find('.load_more_ajax_cont .listing_load_more').fadeIn();
        $listingLoader.show();

        // Update active term
        jQuery('.term_bar_item').removeClass('active_term');
        $this.addClass('active_term');

        // Prepare AJAX data
        var ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';
        var $termBarWrapper = jQuery('.term_bar_wrapper');
        var data = {
            action: 'wpestate_agent_listings',
            term_name: $this.data('term_name'),
            agent_id: $termBarWrapper.data('agent_id'),
            post_id: $termBarWrapper.data('post_id'),
            wpestate_no_listins_per_row: jQuery('.wpestate_single_agent_details_wrapper_type2').length > 0 ? 4 : 3,
            security: jQuery('#wpestate_agent_listings_nonce').val()
        };

        // Clear existing listings
        $agencyListingsWrapper.empty();

        // Perform AJAX request
        jQuery.ajax({
            type: 'POST',
            url: ajaxurl,
            data: data,
            success: function (response) {
                $listingParent.hide();
                $listingLoader.hide();
                $agencyListingsWrapper.append(response);
                wpestate_restart_js_after_ajax();
            },
            error: function (errorThrown) {
                console.error('Error loading agent listings:', errorThrown);
                // Optionally, display an error message to the user
            }
        });
    });
}

/**
 * Returns ACF fields from a given container.
 *
 * This function collects input, textarea, and select elements within the specified container
 * and returns an Array containing their values, formatted according to ACF field naming conventions.
 *
 * @param {string} container - The CSS selector for the container to search within.
 * @returns {Array} acf_fields - An Array containing ACF field names and their corresponding values.
 */
function wpestate_return_form_acf_fields( container = '') {

    var acf_fields = {};

    if ( !jQuery(container).length ) {
        return acf_fields;
    }

    jQuery(container).find('input, textarea, select').each(function() {
        var $field = jQuery(this);
        var fieldName = $field.attr('name');
        
        if (!fieldName || !fieldName.startsWith('acf_fields[')) {
            return;
        }
        
        // Extract field name from acf_fields[field_name] format
        var matches = fieldName.match(/acf_fields\[([^\]]+)\]/);
        if (!matches) {
            return;
        }
        
        var cleanFieldName = matches[1];
        
        if ($field.is(':checkbox')) {
            // Handle checkboxes
            if ($field.attr('name').includes('[]')) {
                // Multiple checkboxes (checkbox field type)
                if (!acf_fields[cleanFieldName]) {
                    acf_fields[cleanFieldName] = [];
                }
                if ($field.is(':checked')) {
                    acf_fields[cleanFieldName].push($field.val());
                }
            } else {
                // Single checkbox (true/false field type)
                acf_fields[cleanFieldName] = $field.is(':checked') ? $field.val() : '';
            }
        } else if ($field.is(':radio')) {
            // Handle radio buttons
            if ($field.is(':checked')) {
                acf_fields[cleanFieldName] = $field.val();
            }
        } else {
            // Handle other input types
            acf_fields[cleanFieldName] = $field.val();
        }
    });
    return acf_fields;
}
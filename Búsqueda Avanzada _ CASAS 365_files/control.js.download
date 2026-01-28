/*global $,Modernizr, jQuery, ajaxcalls_vars,wpestate_custom_search_start_filtering_ajax,imagesLoaded,wpestate_restart_js_after_ajax, panorama,wpestate_toggleStreetView,wpestate_start_filtering_ajax,map,google,wpestate_new_open_close_map,document,initialGeop,placeCircle,wpestate_custom_search_start_filtering_ajax,first_time_wpestate_show_inpage_ajax_half,wpestate_show_pins, wpestate_load_stats,wpestate_enable_half_map_pin_action,wpestate_change_map_type,wpestate_load_stats_tabs,control_vars,mapfunctions_vars, wpestate_map_shortcode_function,grecaptcha,window*/
var width,height;
width   = jQuery(window).width();
height  = jQuery(window).height();
var scroll_trigger  =   0;
var scroll_modal_save,scroll_modal;
var modal_url       = window.location.href;
var modal_title     = document.getElementsByTagName("title")[0];
var slider_counter = 0;



jQuery(window).bind("load", function() {
    wpestate_lazy_load_carousel_property_unit();
});


if(document.getElementById("search_wrapper")){
    var searchbar       = document.getElementById("search_wrapper");
    var sticky_search   = searchbar.offsetTop;
}



var switch_logo;
switch_logo = jQuery('.header_wrapper_inside').attr('data-sticky-logo');

if(jQuery('.header5_top_row').length > 0){
    switch_logo = jQuery('.header5_top_row').attr('data-sticky-logo');
}
if(wpestate_isRetinaDisplay()){
    switch_logo=wpestate_get_custom_retina_pin(switch_logo);
    
}


var switch_logo_original = jQuery('.header_wrapper_inside').attr('data-logo');
if(jQuery('.header5_top_row').length > 0){
    switch_logo_original = jQuery('.header5_top_row').attr('data-logo');
}

if(wpestate_isRetinaDisplay()){
    switch_logo_original=wpestate_get_custom_retina_pin(switch_logo_original);
}

function wpestate_get_custom_retina_pin(custom_img){
    "use strict";
    
    if(typeof(custom_img) === "undefined"){
        return;
    }
    var custom_img_retina;
    custom_img_retina=custom_img.replace(".png","_2x.png");

    jQuery.get(custom_img_retina)
        .done(function() {
            custom_img = custom_img_retina;
        }).fail(function() {
        });
    return custom_img;
}




jQuery(window).scroll(function () {
    "use strict";
    var scroll =scroll_modal= jQuery(window).scrollTop();

    if(control_vars.stiky_search==='yes'){
        wpestate_adv_search_sticky(scroll);
    }else{
        wpestate_header_sticky(scroll);
    }


    if (scroll >= control_vars.scroll_trigger) {
        jQuery('.contact-box').addClass('islive');
        jQuery('.backtop').addClass('islive');
    }else{
        jQuery('.contact-box ').removeClass('islive');
        jQuery('.backtop').removeClass('islive');
        jQuery('.contactformwrapper').addClass('hidden');
    }
    jQuery('.contact_close_button').on( 'click', function(event) {
        event.preventDefault();
        jQuery('.contactformwrapper').addClass('hidden');
    });

});






var sticky_search_local = document.getElementById("search_wrapper") ? document.getElementById("search_wrapper").offsetTop : 0;
var search_height = jQuery('#search_wrapper').height();



/*
* When we advanced search sticky
*
*
*/
// Global variable to store the scroll position at which the searchbar
// should be considered as "exiting" the viewport
var searchbarExitingThreshold;

/**
 * Calculates the threshold at which the searchbar is considered to be exiting the viewport.
 * This function should be called on page load and window resize to ensure accuracy.
 */
function calculateSearchbarThreshold() {
    "use strict";
    var searchbar = jQuery('.search_wrapper_sticky_search');
    if (searchbar.length > 0) {
        // Get the initial top position of the searchbar
        var searchbarInitialPosition = searchbar.offset().top;
        // Get the height of the searchbar
        var searchbarHeight = searchbar.outerHeight();
        // Calculate the threshold by adding the initial position and height
        searchbarExitingThreshold = searchbarInitialPosition + searchbarHeight;
      
    }
}

/**
 * Main function to handle the sticky behavior of the advanced search bar.
 * This function is called on scroll events.
 * 
 * @param {number} scroll - The current scroll position of the window.
 */
function wpestate_adv_search_sticky(scroll) {
    "use strict";

    // Early return if the search wrapper doesn't exist
    if (!document.getElementById("search_wrapper")) {
        return;
    }
    // Early return for mobile devices (screens smaller than 1024px)
    if (screen.width < 1024) {
        return;
    }

    var searchbar = jQuery('.search_wrapper_sticky_search.with_search_form_float');
    // Early return if the searchbar doesn't exist
    if (searchbar.length === 0) {
        return;
    }

    if (scroll >= searchbarExitingThreshold) {
        // The searchbar is exiting or has exited the screen
        if (!searchbar.hasClass('sticky_adv_anime')) {
            // Add the class only if it's not already present
            searchbar.addClass('sticky_adv_anime');
      
        }
    } else {
        // The searchbar is visible on the screen
        if (searchbar.hasClass('sticky_adv_anime')) {
            // Remove the class only if it's present
            searchbar.removeClass('sticky_adv_anime');
       
        }
    }
}









function wpestate_header_sticky(scroll){

    "use strict";

    var sticky_class="master_header_sticky";
    var control_vars_scroll_trigger=parseInt(control_vars.scroll_trigger);

    if(control_vars.content_type!='' && control_vars.content_type!=='tabs' && jQuery('.wpestate_top_property_navigation_link').length!==0){
         sticky_class=sticky_class+" "+"sticky_property_menu"
    }
    

    if(jQuery('.admin-bar').length>0){
        control_vars_scroll_trigger=control_vars_scroll_trigger+32;
    }
            
    if (scroll >= control_vars_scroll_trigger) {
        if (!Modernizr.mq('only all and (max-width: 1023px)')) {

            jQuery(".master_header").addClass(sticky_class);
            jQuery('.logo').addClass('miclogo');
            if( switch_logo!=='' ){
                jQuery('#logo_image').attr('src',switch_logo);
            }

            if( !jQuery(".header_wrapper").hasClass('header_type4') ){
                jQuery(".header_wrapper").addClass("navbar-fixed-top");
     
            }
        
            jQuery('#user_menu_open').hide();
            jQuery('.navicon-button').removeClass('opensvg');
            jQuery('#wpestate_header_shoping_cart').hide();

            jQuery('.wpestate_top_property_navigation').addClass('sticky_property_menu_visible');

        }

    } else {
        jQuery(".master_header").removeClass(sticky_class);
        jQuery(".header_wrapper").removeClass("navbar-fixed-top");


      
        jQuery('#user_menu_open').hide();
        jQuery('#wpestate_header_shoping_cart').hide();
        jQuery('.logo').removeClass('miclogo');

        jQuery('.wpestate_top_property_navigation').removeClass('sticky_property_menu_visible');


        if( switch_logo!=='' ){
           
            jQuery('#logo_image').attr('src',switch_logo_original);
        }
    }
}



function wpestate_isRetinaDisplay() {
    "use strict";
    if (window.matchMedia) {
        var mq = window.matchMedia("only screen and (min--moz-device-pixel-ratio: 1.3), only screen and (-o-min-device-pixel-ratio: 2.6/2), only screen and (-webkit-min-device-pixel-ratio: 1.3), only screen  and (min-device-pixel-ratio: 1.3), only screen and (min-resolution: 1.3dppx)");
        return (mq && mq.matches || (window.devicePixelRatio > 1));
    }
}


/**
 * Property Navigation Scroll Handler
 * 
 * Manages smooth scrolling to property sections and highlights
 * active navigation items based on scroll position.
 * 
 * Dependencies:
 * - jQuery
 * - .wpestate_top_property_navigation element with anchor links
 * - .property-panel elements with matching IDs
 */

function wpestate_property_sticky() {
    'use strict';
    
    const SCROLL_OFFSET = 80;
    const SCROLL_DURATION = 500;
    const $navigation = jQuery('.wpestate_top_property_navigation a');

    // Handle navigation click events
    $navigation.on('click', function(event) {
        event.preventDefault();
        
        // Get target section
        const targetId = jQuery(this).attr('href');
        const $targetElement = jQuery(targetId);
        
        // Update active state
        $navigation.removeClass('item_active');
        jQuery(this).addClass('item_active');
        
        // Smooth scroll to target
        jQuery('html, body').animate({
            scrollTop: $targetElement.offset().top - SCROLL_OFFSET
        }, SCROLL_DURATION);
    });
    
    // Handle scroll events
    jQuery(window).on('scroll', function() {
        const scrollTop = jQuery(window).scrollTop() + SCROLL_OFFSET;
        let currentSection = null;

        // Find the current section
        jQuery('.property-panel').each(function() {
            const sectionTop = jQuery(this).offset().top;
            const sectionBottom = sectionTop + jQuery(this).outerHeight();
            
            if (scrollTop >= sectionTop && scrollTop < sectionBottom) {
                currentSection = this.id;
                return false; // Break the loop when found
            }
        });

        // Update active state only if a section is found
        $navigation.removeClass('item_active');
        jQuery('.wpestate_top_property_navigation a').removeClass('item_active');
      
        if (currentSection) {
            $navigation.filter(`[href="#${currentSection}"]`).addClass('item_active');
         
        }
    });
}


jQuery(window).resize(function() {
    "use strict";
    if(jQuery(window).width() != width ){
        jQuery('#mobile_menu').hide('10');
    }
    wpestate_adjustMegaMenuPosition();
    wpestate_half_map_responsive();
    wpestat_resize_wpestate_property_slider_v2();

});

function wpestate_half_map_responsive(){
    if (Modernizr.mq('only screen and (min-width: 640px)') && Modernizr.mq('only screen and (max-width: 1025px)')) {
        var half_map_header = jQuery('.master_header ').height();
    }

}

function wpestate_half_map_controls(){

    jQuery('.half_mobile_toggle_listings').on('click',function(){

        jQuery('.half_map_controllers_wrapper div').removeClass('half_control_visible');
        jQuery(this).addClass('half_control_visible');
        jQuery('#google_map_prop_list_wrapper').removeAttr('style');
        jQuery('#google_map_prop_list_sidebar').show();
        jQuery('.half_mobile_hide').hide();
        jQuery('#colophon').show();
        wpestate_adjust_sticky_footer();
    });


     jQuery('.half_mobile_toggle_map').on('click',function(){

        jQuery('.half_map_controllers_wrapper div').removeClass('half_control_visible');
        jQuery(this).addClass('half_control_visible');
        jQuery('#google_map_prop_list_sidebar').hide();
        jQuery('#google_map_prop_list_wrapper').css('height','100%');
        jQuery('#google_map_prop_list_wrapper').css('position','relative');
        jQuery('#googleMap').css('min-height','100vh');

        // Hide footer
        jQuery('#colophon').hide();
        jQuery('.wpresidence_main_wrapper_class').css('margin-bottom','0px');


        jQuery('.half_mobile_hide').show();

        wpresidence_map_general_fit_to_bounds(1);
      if(wp_estate_kind_of_map===1){

    }else if(wp_estate_kind_of_map===2){
      map.invalidateSize();
        map.fitBounds(bounds_list);
    }
    });
}





Number.prototype.format = function(n, x) {
    var re = '\\d(?=(\\d{' + (x || 3) + '})+' + (n > 0 ? '\\.' : '$') + ')';
    return this.toFixed(Math.max(0, ~~n)).replace(new RegExp(re, 'g'), '$&'+control_vars.price_separator);
};

function wpestate_element_hide(item){
    "use strict";
    jQuery(document).on( 'click', function(event) {
        if (!jQuery(item).is(event.target) && jQuery(item).has(event.target).length === 0 ){
            jQuery('#results').fadeOut();

        }
    });
}
wpestate_element_hide('.search_wrapper');

function wpestate_lazy_load_carousel_property_unit(){
    "use strict";

    jQuery('.property_unit_carousel img').each(function(event){
          var new_source='';
          new_source=jQuery(this).attr('data-lazy-load-src');
          if(typeof (new_source)!=='undefined' && new_source!==''){

            jQuery(this).attr('src',new_source);
          }
      });
}
 //recaptha


function wpestate_card_unit_contact_actions(){


    jQuery('.wpestate_property_card_contact_wrapper_phone').on('click',function(event){
        event.stopPropagation(); 
       
        var prop_id = jQuery(this).attr('data-item-id');
        var parent = jQuery(this).parent().parent();
       
        parent.find('.wpestate_card_unit_call_'+prop_id).appendTo("body");
        jQuery('body> .wpestate_card_unit_call_'+prop_id).first().modal("show");
    });

    jQuery('.wpestate_property_card_contact_wrapper_email').on('click',function(event){
        event.stopPropagation();  
        var prop_id = jQuery(this).attr('data-item-id');
        var parent = jQuery(this).parent().parent();
        parent.find('.wpestate_card_unit_email_'+prop_id).appendTo("body");
        jQuery('body> .wpestate_card_unit_email_'+prop_id).first().modal("show");
    });

    jQuery('.wpestate_property_card_contact_wrapper_whatsupp').on('click',function(event){
        event.stopPropagation();
      
    });

}






wpestate_advnced_filters_bars();

function wpresidence_advanced_searc_6_actions(){
      ////////// adv serach 6
    jQuery('.adv6_tab_head,.elementor_search_tab_head').on( 'click', function(event) {
        
   

        var tab_controls;
        var parent=jQuery(this).parent().parent();
        parent.find('.adv_search_tab_item').removeClass('active');
        jQuery(this).parent().addClass('active');
        
        
        tab_controls = jQuery(this).attr('aria-controls');
        jQuery('.adv6_price_low').removeClass('price_active');
        jQuery('.adv6_price_max').removeClass('price_active');

        jQuery('#'+tab_controls).find('.adv6_price_low').addClass('price_active');
        jQuery('#'+tab_controls).find('.adv6_price_max').addClass('price_active');

    });

}



/**
 * Trigger Login Modal Open Action
 * 
 * This function sets up an event listener for the login trigger modal button.
 * When clicked, it calls the function to open the login modal.
 *
 * @since 1.0.0
 * 
 * @requires jQuery
 * @requires wpestate_trigger_login_modal_open
 * 
 * @return {void}
 */
function wpestate_trigger_login_modal_open_action() {
    jQuery('#login_trigger_modal').on('click', function(event) {
        wpestate_trigger_login_modal_open(2);
    });
}

/**
 * Open Login Modal
 * 
 * This function handles the opening of the login modal. It's used in various scenarios
 * such as saving to favorites, saving a search, or adding a property when login is required.
 * The function behaves differently based on the screen size (desktop vs mobile).
 *
 * @since 1.0.0
 * 
 * @requires jQuery
 * @requires Modernizr
 * 
 * @return {void}
 */
function wpestate_trigger_login_modal_open(stayOnPage) {
    // Check if the screen width is greater than 768px (non-mobile devices)
    // stayOnPage is a flag that indicates if the page should be reloaded after login 1-yes, 0-no will reload page from theme option , 2 - no will reload current page
    if(!wpestate_isMobileOrTablet()){
        // Desktop behavior
        var login_modal = jQuery('#modal_login_wrapper');
        
        // Show the login modal
        login_modal.show();
        // Set focus to the first focusable element in the modal
        login_modal.find('[autofocus]').focus();
        
        // Set the loginpop value to 1 (indicating a popup login)
        login_modal.find('.loginpop').val(stayOnPage);
    } else {
        // Mobile behavior
        // Trigger the mobile user menu
        jQuery('.mobile-trigger-user').trigger('click');
        
        // Set the loginpop value to 1 in the mobile wrapper
        jQuery('.mobilewrapper-user .loginpop').val(stayOnPage);
    }
}


function wpestate_bootstrap53_dropdown(){
    jQuery('.wpresidence_dropdown .dropdown-menu li').on('click', function() {
        // Get the selected value and label
        var selectedValue = jQuery(this).data('value');
        var selectedLabel = jQuery(this).text();

        // Update the button text
        jQuery(this).closest('.wpresidence_dropdown').find('.dropdown-toggle').text(selectedLabel);

        // Update the hidden input value
        jQuery(this).closest('.wpresidence_dropdown').find('input[type="hidden"]').val(selectedValue);

        // Optionally close the dropdown after selection
        jQuery(this).closest('.dropdown').removeClass('show');
        jQuery(this).closest('.dropdown').find('.dropdown-menu').removeClass('show');
    });
}



jQuery(document).ready(function () {
    "use strict";

    wpestate_bootstrap53_dropdown();
    wpestate_initializeTooltips_bootstrap53();

    // Calculate the initial threshold for advanced search sticky
    calculateSearchbarThreshold();  
    // Get the initial scroll position
    var initialScroll = jQuery(window).scrollTop();
    // Call the function to set the correct initial state for advanced search sticky
    wpestate_adv_search_sticky(initialScroll);

    // trigger login modal open 
    wpestate_trigger_login_modal_open_action();


   jQuery('#login_user_wd').focus({ preventScroll: true });

    if(jQuery('.wpestate_anime').length>0 ){
        jQuery('.wpestate_anime').each( function(){
            var element_id = jQuery(this).attr('id');
            wpestate_property_list_sh('#'+element_id+' .wpestate_item_list_sh', '#'+element_id+' .control_tax_sh');
        });

    }
    wpestate_change_currency_dropdown_action();
    wpestate_adjustMegaMenuPosition();
    wpestate_reposion_dropdowns();
    wpestate_beds_baths_component();
    wpestate_price_component_item();
    wpestate_price_component_item_v3();

    wpestate_blog_list_widget();
    wpestate_get_location();
    wpestate_grid_list_controls();

    wpestate_slider_box();
    wpestate_enable_share_unit();
    wpestate_control_media_buttons();
    wpestate_card_unit_contact_actions();
    wpestate_morgate_widget_action();
    wpestate_zillow_widget_action();
    wpestate_save_search_actions();
    wpestate_delete_save_search_actions();
    wpestate_overview_map();
    wpestate_extended_search_function();
    wpresidence_advanced_searc_6_actions();
    wpestate_show_hide_search_form_hidden_fields();
    wpestate_adjust_sticky_footer();
    wpestate_map_types_control();
    

    ////////////////////////////////////////////////////////////////////////////////////////////
    ///city-area-selection
    ///////////////////////////////////////////////////////////////////////////////////////////

    wpestate_filter_city_area('filter_city','filter_area');
    wpestate_filter_city_area('sidebar-adv-search-city','sidebar-adv-search-area');
    wpestate_filter_city_area('adv-search-city','adv-search-area');
    wpestate_filter_city_area('half-adv-search-city','half-adv-search-area');
    wpestate_filter_city_area('shortcode-adv-search-city','shortcode-adv-search-area');
    wpestate_filter_city_area('mobile-adv-search-city','mobile-adv-search-area');
    wpestate_filter_city_area('property_city_submit', 'property_area_submit');
    wpestate_filter_city_area('a_filter_cities', 'a_filter_areas');
    wpestate_filter_city_area('advanced_city', 'advanced_area');

    ////////////////////////////////////////////////////////////////////////////////////////////
    //county-city-selection
    ///////////////////////////////////////////////////////////////////////////////////////////
    wpestate_filter_county_city( 'filter_county', 'filter_city' );
    wpestate_filter_county_city( 'sidebar-adv-search-countystate', 'sidebar-adv-search-city' );
    wpestate_filter_county_city( 'adv-search-countystate', 'adv-search-city' );
    wpestate_filter_county_city( 'half-adv-search-countystate', 'half-adv-search-city' );
    wpestate_filter_county_city( 'shortcode-adv-search-countystate', 'shortcode-adv-search-city' );
    wpestate_filter_county_city( 'mobile-adv-search-countystate', 'mobile-adv-search-city' );
    wpestate_filter_county_city( 'a_filter_county', 'a_filter_cities' );
    wpestate_filter_county_city( 'county-state', 'advanced_city' );
 


    if(jQuery('.googleMap_shortcode_class').length>0 ){
        if(  jQuery(".single-estate_property").length==0 ){
            wpestate_map_shortcode_function();
        }else{
            if(  jQuery("#googleMap").length==0 &&  jQuery("#googleMapSlider").length==0 ){
            }
        }
    }


    jQuery('body').on('click', '.add_custom_parameter', function(){
        const cloned = `
        <div class="cliche_row row">
            <div class="col-md-5">
                <label for="agent_custom_label">${control_vars.agent_field_name}</label>
                <input type="text" class="form-control agent_custom_label" value="" name="agent_custom_label[]">
            </div>
            <div class="col-md-5">
                <label for="agent_custom_value">${control_vars.agent_field_value}</label>
                <input type="text" class="form-control agent_custom_value" value="" name="agent_custom_value[]">
            </div>
            <div class="col-md-2">
                <button type="button" class="wpresidence_button remove_parameter_button">${control_vars.remove}</button>
            </div>
        </div>
    `;
    
        jQuery('input', cloned).val('');
        jQuery('.wpresidence_button.add_custom_parameter').before( cloned );
    });




    jQuery('body').on('click', '.remove_parameter_button', function(){
        var pnt = jQuery(this).parents( '.cliche_row' );
        pnt.fadeOut(500, function(){
                pnt.replaceWith('');
        });
    });


    jQuery('.buy_package_sh a').on( 'click', function(event) {
        if (parseInt(ajaxcalls_vars.userid, 10) === 0 ) {
            event.preventDefault();
            jQuery('#modal_login_wrapper').show();
             jQuery('#modal_login_wrapper').find('[autofocus]').focus();
            jQuery('.loginpop').val('1');
        }
    });

    if (typeof wpestate_enable_half_map_pin_action == 'function'){
        wpestate_enable_half_map_pin_action();
    }  

   

    var screen_width,screen_height,map_tab;
    if (jQuery.datepicker && jQuery.datepicker.regional && jQuery.datepicker.regional[control_vars.datepick_lang]) {
        jQuery.datepicker.setDefaults( jQuery.datepicker.regional[control_vars.datepick_lang] );
    }



    estate_splash_slider();


    var new_height;
    if (jQuery(".full_screen_yes").length) {

        if( jQuery('.header_transparent').length > 0){
            new_height = jQuery( window ).height();
        }else{
            new_height = jQuery( window ).height() - jQuery('.master_header').height();
        }

        if( jQuery('.with_search_on_start').length>0 ){
            new_height=new_height- jQuery('.search_wrapper.with_search_on_start ').height();
        }

        jQuery('.wpestate_header_image,.wpestate_header_video,.theme_slider_wrapper,.theme_slider_classic,.theme_slider_wrapper .item_type2 ').css('height',new_height);
    }


   



    jQuery('.wp-block-residence-gutenberg-block-testimonial-slider').each(function(){

       if( jQuery(this).find('.type_class_3 ').length>0 ){
           jQuery(this).addClass('container_type_3');
       }

    })






    jQuery('#preview_view_all').on( 'click', function(event) {

        if( (mapfunctions_vars.adv_search_type==='6' || mapfunctions_vars.adv_search_type==='7' || mapfunctions_vars.adv_search_type==='8' || mapfunctions_vars.adv_search_type==='9' ) ){

            jQuery('.search_wrapper .tab-pane.active .wpresidence_button').trigger('click');
        }else{
            jQuery('.search_wrapper .wpresidence_button').trigger('click');
        }
    });


    jQuery('.theme_slider_2 .prop_new_details ').on( 'click', function(event) {

        var new_link;
        new_link =  jQuery(this).attr('data-href');
        window.open (new_link,'_self',false);
    });

    jQuery('.theme_slider_classic').on( 'click', function(event) {

       if (event.target == this)  {
            var new_link;
            new_link =  jQuery(this).attr('data-href');
            window.open (new_link,'_self',false);
        }
    });

    setTimeout(function() {   wpresidence_list_view_arrange(); }, 300);

    ////////// map shortcode
    map_tab=0;
    jQuery('#propmaptrigger').on( 'click', function(event) {
        if(map_tab===0){

            wpestate_map_shortcode_function();
            map_tab=1;

        }
    });


    setTimeout(function(){
        jQuery('.theme_slider_2 .theme_slider_2_contact_agent').fadeIn();
    }, 700);


	////////////////////////////
	// taxonomy slick slider
	////////////////////////////
    wpestate_half_map_responsive();
    wpestate_half_map_controls();

    jQuery('.show_stats').on( 'click', function(event) {
        event.preventDefault();
        var parent,listing_id;
        parent = jQuery(this).parent().parent().parent();
        listing_id = jQuery(this).attr('data-listingid');
        //jQuery('.statistics_wrapper').slideUp();

        if( parent.find('.statistics_wrapper').hasClass('is_slide')  ){
               parent.find('.statistics_wrapper').slideUp().removeClass('is_slide');
        }else{
            parent.find('.statistics_wrapper').slideDown().addClass('is_slide');
            wpestate_load_stats(listing_id);
        }




    });

     jQuery('.tabs_stats,#1462452319500-8587db8d-e959,#1462968563400-b8613baa-7092').on( 'click', function(event) {
       var parent,listing_id;
       listing_id = jQuery(this).attr('data-listingid');
       if(typeof(listing_id)==='undefined'){
           listing_id =  jQuery('.estate_property_first_row').attr('data-prp-listingid');
       }


       wpestate_load_stats_tabs(listing_id);
    });



    ////////////////////////////////////////////////////////////////////////////
    //new retina script
    ////////////////////////////////////////////////////////////////////////////

        jQuery('.retina_ready').dense({
            'ping'      : true,
            'dimension' : 'preserve'
        });
        var image_unnit = jQuery('<div data-1x="'+control_vars.path+'/css/css-images/unit.png" data-2x="'+control_vars.path+'/css/css-images/unit_2x.png" />').dense('getImageAttribute');
        var image_unnit = jQuery('<div data-1x="'+control_vars.path+'/css/css-images/unitshare.png" data-2x="'+control_vars.path+'/css/css-images/unitshare_2x.png" />').dense('getImageAttribute');

    ////////////////////////////////////////////////////////////////////////////
    //invoice filters
    ////////////////////////////////////////////////////////////////////////////


   jQuery(function() {
        jQuery("#invoice_start_date,#invoice_end_date").datepicker({
            dateFormat : "yy-mm-dd",
        }).datepicker('widget').wrap('<div class="ll-skin-melon"/>');
    });


    ////////////////////////////////////////////////////////////////////////////
    //new mobile menu 1.10
    ////////////////////////////////////////////////////////////////////////////

    jQuery('.all-elements').animate({
            minHeight: 100+'%'
    });

    jQuery('.header-tip').addClass('hide-header-tip');

    wpestate_mobile_menu_slideout();
    wpestate_mobile_menu_open_submenu();
    wpestate_elementor_mobile_menu_trigger_open();


   


   

    ////////////////////////////////////////////////////////////////////////////
    // easure unit cookige
    ////////////////////////////////////////////////////////////////////////////

    jQuery('.list_sidebar_measure_unit li').on( 'click', function(event) {
        var ajaxurl,value, pick;
        value = jQuery(this).attr('data-value');

        var parent_pointer = jQuery(this).parents('.dropdown ');
        pick = jQuery(this).text();
        jQuery('.sidebar_filter_menu', parent_pointer).text(pick).append('<span class="caret caret_sidebar"></span>');
        var nonce = jQuery('#wpestate_change_measure').val();
        ajaxurl     =   ajaxcalls_vars.admin_url + 'admin-ajax.php';
        jQuery.ajax({
            type: 'POST',
            url: ajaxurl,
            data: {
                'action'    :   'wpestate_set_cookie_measure_unit',
                'value'     :   value,
                'security'  : nonce
            },
            success: function (data) {

               location.reload();
            },
            error: function (errorThrown) {}
        });//end ajax

    });










    ////////////////////////////////////////////////////////////////////////////
    /// slider price
    ////////////////////////////////////////////////////////////////////////////

    var price_low_val= parseInt( jQuery('#price_low').val() );
    var price_max_val= parseInt( jQuery('#price_max').val() );
    var my_custom_curr_symbol  =   decodeURI ( wpestate_getCookie('my_custom_curr_symbol') );
    var my_custom_curr_coef    =   parseFloat( wpestate_getCookie('my_custom_curr_coef'));
    var my_custom_curr_pos     =   parseFloat( wpestate_getCookie('my_custom_curr_pos'));
    var my_custom_curr_cur_post=   wpestate_getCookie('my_custom_curr_cur_post');


    wpestate_enable_slider('slider_price', 'price_low', 'price_max', 'amount', my_custom_curr_pos, my_custom_curr_symbol, my_custom_curr_cur_post,my_custom_curr_coef);
    jQuery( "#slider_price" ).slider({
        stop: function( event, ui ) {
            if (typeof (wpestate_show_pins) !== "undefined") {
                first_time_wpestate_show_inpage_ajax_half=1;

                wpestate_show_pins();
            }
        }
    });
    wpestate_enable_slider('slider_price_sh', 'price_low_sh', 'price_max_sh', 'amount_sh', my_custom_curr_pos, my_custom_curr_symbol, my_custom_curr_cur_post,my_custom_curr_coef);
    wpestate_enable_slider('slider_price_widget', 'price_low_widget', 'price_max_widget', 'amount_wd', my_custom_curr_pos, my_custom_curr_symbol, my_custom_curr_cur_post,my_custom_curr_coef);
    wpestate_enable_slider('slider_price_mobile', 'price_low_mobile', 'price_max_mobile', 'amount_mobile', my_custom_curr_pos, my_custom_curr_symbol, my_custom_curr_cur_post,my_custom_curr_coef);

  
    wpestate_enable_slider_elementor();

    if(control_vars.adv6_taxonomy_term!==''){
        control_vars.adv6_taxonomy_term.forEach(wpestate_advtabs_function);
    }





    ////////////////////////////////////////////////////////////////////////////





    jQuery('#estate-carousel .slider-content h3 a,#estate-carousel .slider-content .read_more ').on( 'click', function(event) {
      var new_link;
      new_link =  jQuery(this).attr('href');
      window.open (new_link,'_self',false);
    });


  



    ////////////////////////////////////////////////////////////////////////////////////////////
    ///mobile
    ///////////////////////////////////////////////////////////////////////////////////////////


    jQuery('#adv-search-header-mobile').on( 'click', function(event) {
        jQuery('#adv-search-mobile').fadeToggle('300');

    });


    ////////////////////////////////////////////////////////////////////////////////////////////
    ///navigational links
    ///////////////////////////////////////////////////////////////////////////////////////////

    jQuery('.nav-prev,.nav-next ').on( 'click', function(event) {
        event.preventDefault();
        var link = jQuery(this).find('a').attr('href');
        window.open (link,'_self',false);
    });

    ////////////////////////////////////////////////////////////////////////////////////////////
    /// featured agent
    ///////////////////////////////////////////////////////////////////////////////////////////


    jQuery('.featured_agent_details_wrapper, .agent-listing-img-wrapper').on( 'click', function(event) {
        var newl= jQuery( this ).attr('data-link');
        window.open (newl,'_self',false);
    });

    jQuery('.see_my_list_featured').on( 'click', function(event) {
            event.stopPropagation();
    });

    ////////////////////////////////////////////////////////////////////////////////////////////
    /// featuerd property
    ///////////////////////////////////////////////////////////////////////////////////////////

    jQuery('.featured_cover').on( 'click', function(event) {
        var newl= jQuery( this ).attr('data-link');
        window.open (newl,'_self',false);
    });


    jQuery( '.agent_face' )
        .on('mouseenter', function(){   jQuery(this).find('.agent_face_details').fadeIn('500'); })
        .on('mouseleave', function(){  jQuery(this).find('.agent_face_details').fadeOut('500'); });


    ////////////////////////////////////////////////////////////////////////////////////////////
    /// listings unit navigation
    ///////////////////////////////////////////////////////////////////////////////////////////
    jQuery('.places_cover,.agent_unit, .blog_unit , .featured_widget_image,.blog3v .property_listing_blog,.featured_img_type2').on( 'click', function(event) {
        var link;
        link = jQuery(this).attr('data-link');

        window.open(link, '_self');
    });



    jQuery('.property_listing').on( 'click', function(event) {
       
        if (
            event.target.classList.contains('demo-icon') || 
            event.target.classList.contains('carousel-control-next-icon') || 
            event.target.classList.contains('carousel-control-prev-icon')   
        ){
        
            return;
        }
  
        if(control_vars.property_modal === '1' && !Modernizr.mq('only all and (max-width: 1024px)') ){
            event.preventDefault();
            event.stopPropagation();
            scroll_modal_save=scroll_modal;

            var listing_id  =   jQuery(this).parent().attr('data-listid');
            var main_img_url=   jQuery(this).parent().attr('data-main-modal');
            var main_title  =   jQuery(this).parent().attr('data-modal-title');
            var link        =   jQuery(this).parent().attr('data-modal-link');
            wpestate_enable_property_modal(listing_id,main_img_url,main_title,link);
        }else{

            if(control_vars.new_page_link==='_blank' && !jQuery(this).hasClass('places_listing')){
                return;
            }
            var link;
            link = jQuery(this).attr('data-link');

          window.open(link, '_self');
        }
    });






    jQuery('.share_unit').on( 'click', function(event) {
        event.stopPropagation();
    });

    jQuery('.related_blog_unit_image').on( 'click', function(event) {
         var link;
        link = jQuery(this).attr('data-related-link');
        window.open(link, '_self');
    });

    ////////////////////////////////////////////////////////////////////////////////////////////
    /// user menu
    ///////////////////////////////////////////////////////////////////////////////////////////

    wpestate_open_menu();

    jQuery('#wpresidence_elementor_register').on( 'click', function(event) {

        event.preventDefault();
        jQuery('.login-links').show();
  
        jQuery('#modal_login_wrapper').show();      
        event.preventDefault();
        jQuery('#login-div_topbar,#widget_register_topbar,#login-div-title-topbar,#forgot-div-title-topbar,#forgot-pass-div').hide();
        jQuery('#register-div-topbar,#register-div-title-topbar,#widget_login_topbar,#forgot_pass_topbar').show().find('[autofocus]').focus();

        jQuery('#modal_login_wrapper').find('[autofocus]').focus();
    });

    jQuery('#wpresidence_elementor_login').on( 'click', function(event) {

        event.preventDefault();
        jQuery('.login-links').show();
        jQuery('#modal_login_wrapper').show();
        jQuery('#login-div_topbar,#widget_register_topbar,#login-div-title-topbar,#forgot_pass_topbar').show().find('[autofocus]').focus();
        jQuery('#register-div-topbar,#register-div-title-topbar,#widget_login_topbar,#forgot-div-title-topbar,#forgot-pass-div').hide();
  

        jQuery('#modal_login_wrapper').find('[autofocus]').focus();
    });


   

    jQuery('#user_menu_u.user_not_loged .submit_action').on( 'click', function(event) {
        jQuery('.login-links').show();
        jQuery('#modal_login_wrapper').show();
        jQuery('#modal_login_wrapper').find('[autofocus]').focus();
    });

   

    jQuery('#property_details_modal_close').on('click',function(event){
        jQuery('.website-wrapper').removeAttr('style');
        window.scrollTo(0, parseInt(scroll_modal_save) );
        jQuery('#property_details_modal_wrapper').hide();
        window.history.pushState("", modal_title,modal_url);
    });

    jQuery('#shopping-cart').on('click',function (event) {

        jQuery('#user_menu_open').removeClass('iosfixed').fadeOut(400);
        if (jQuery('#wpestate_header_shoping_cart').is(":visible")) {
            jQuery('#wpestate_header_shoping_cart').fadeOut(400);
        } else {
            jQuery('#wpestate_header_shoping_cart').fadeIn(400);
        }
        event.stopPropagation();
    });


    jQuery(document).on( 'click', function(event) {

        var clicka  =   event.target.id;
        var clicka2 =   jQuery(event.target).attr('share_unit');
        jQuery('#wpestate_header_shoping_cart').fadeOut(400);
        if ( !jQuery('#'+clicka).parents('.topmenux').length) {
            jQuery('#user_menu_open').removeClass('iosfixed').hide(400);
            jQuery('.navicon-button').removeClass('opensvg');
            jQuery('#user_menu_u .navicon-button').removeClass('open');
        }

        jQuery('.share_unit').hide();


        if (event.target.id == "header_type3_wrapper" || jQuery(event.target).parents("#header_type3_wrapper").size()) {

        } else {
            var css_right   = parseFloat( jQuery('.header_type3_menu_sidebar').css('right') );
            var css_left    = parseFloat( jQuery('.header_type3_menu_sidebar').css('left') );
            if(css_right===0 || css_left===0 ){
                jQuery('.header_type3_menu_sidebar.header_left.sidebaropen').css("right","-300px");
                jQuery('.header_type3_menu_sidebar.header_right.sidebaropen').css("left","-300px");
                jQuery('.container.main_wrapper.has_header_type3').css("padding","0px");
                jQuery('.elementor-section.elementor-top-section ').css("padding-right","0px");
                jQuery('.elementor-section.elementor-top-section ').css("padding-left","0px");
                jQuery('.master_header').removeAttr('style');

            }
        }
    });


    jQuery('#header_type3_trigger').on( 'click', function(event) {

        event.preventDefault();
        if ( !jQuery('.container').hasClass('is_boxed') ){
            if( jQuery('.header_type3_menu_sidebar').hasClass('header_left') ){
                jQuery(".header_type3_menu_sidebar").css("right","0px");
                jQuery(".container.main_wrapper ").css("padding-right","300px");
                jQuery('.elementor-section.elementor-top-section ').css("padding-right","300px");
                jQuery(".master_header").css("right","150px");
            }else{
                jQuery(".header_type3_menu_sidebar").css("left","0px");
                jQuery(".container.main_wrapper ").css("padding-left","300px");
                jQuery('.elementor-section.elementor-top-section ').css("padding-left","300px");
                jQuery(".master_header").css("left","150px");
            
            }
            jQuery(".header_type3_menu_sidebar").addClass("sidebaropen");
        }else{
             if( jQuery('.header_type3_menu_sidebar').hasClass('header_left') ){
                jQuery(".header_type3_menu_sidebar").css("right","0px");

            }else{
                jQuery(".header_type3_menu_sidebar").css("left","0px");

            }
            jQuery(".header_type3_menu_sidebar").addClass("sidebaropen");
        }
    });


    ////////////////////////////////////////////////////////////////////////////////////////////
    /// new controls for upload pictures
    ///////////////////////////////////////////////////////////////////////////////////////////

    jQuery('#imagelist i.fa-trash-alt').on( 'click', function(event) {
          var curent='';
          jQuery(this).parent().remove();

          jQuery('#imagelist .uploaded_images').each(function(){
             curent=curent+','+jQuery(this).attr('data-imageid');
          });
          jQuery('#attachid').val(curent);

      });

    jQuery('#imagelist img').dblclick(function(){

        jQuery('#imagelist .uploaded_images .thumber').each(function(){
            jQuery(this).remove();
        });

        jQuery(this).parent().append('<i class="fa thumber fa-star"></i>');
        jQuery('#attachthumb').val(   jQuery(this).parent().attr('data-imageid') );
    });





    jQuery('#switch').on( 'click', function(event) {
        jQuery('.main_wrapper').toggleClass('wide');
    });


    jQuery('#accordion_prop_addr, #accordion_prop_details, #accordion_prop_features').on('shown.bs.collapse', function () {
        jQuery(this).find('h4').removeClass('carusel_closed');
    });

    jQuery('#accordion_prop_addr, #accordion_prop_details, #accordion_prop_features').on('hidden.bs.collapse', function () {
        jQuery(this).find('h4').addClass('carusel_closed');
    });

    ///////////////////////////////////////////////////////////////////////////////////////////
    //////// advanced search filters
    ////////////////////////////////////////////////////////////////////////////////////////////

    var elems = ['.directory_sidebar','.search_wrapper' , 
        '#advanced_search_shortcode', '#advanced_search_shortcode_2', '.adv-search-mobile','.advanced_search_sidebar'];

    jQuery.each( elems, function( i, elem ) {

        jQuery(elem+' li').on( 'click', function(event) {
            event.preventDefault();
            var pick, value, parent,parent_replace;
            parent_replace='.filter_menu_trigger';
            if(elem === '.advanced_search_sidebar'  ){
                parent_replace='.sidebar_filter_menu';
            }

            pick = jQuery(this).text();
            value = jQuery(this).attr('data-value');

			// agents search patch
			//if( jQuery(this).parents('.advanced_search_sidebar').hasClass('ag_ag_dev_search_widget') ){
			if( jQuery(this).parent().hasClass('aag_picker') ){
				jQuery('.ag_ag_dev_search_selector').hide();
				jQuery('.selector_for_'+value).fadeIn();
			}


            parent = jQuery(this).parent().parent();
            if(elem === '.directory_sidebar' ){
                parent.find(parent_replace).text(pick).attr('data-value',value);
            }else{
                parent.find(parent_replace).text(pick).attr('data-value',value);
            }
            parent.find('input').val(value);
        });
    });




    jQuery('.search_wrapper li, .extended_search_check_wrapper input[type="checkbox"]').on( 'click', function(event) {

        if(jQuery(this).hasClass('wpestate_prevent_ajax')){
            return;
        }

        if (typeof (wpestate_show_pins) !== "undefined") {
            first_time_wpestate_show_inpage_ajax_half=1;
            wpestate_show_pins();
        }
    });





    var typingTimer;                //timer identifier
    var doneTypingInterval = 1500;  //time in ms, 5 second for example
    var jQueryinput = jQuery('#adv_location,.search_wrapper input[type=text]');

    jQueryinput.on('keyup', function (event) {
        if(jQuery(this).attr('id')=='geolocation_search' || jQuery(this).attr('id')=='geolocation_search2') {
            return;
        }
        if (event.keyCode === 13) { 
            first_time_wpestate_show_inpage_ajax_half=1;
            wpestate_show_pins();
        }
    });

    //on keydown, clear the countdown
    jQueryinput.on('keydown', function () {
      clearTimeout(typingTimer);
    });



    ///////////////////////////////////////////////////////////////////////////////////////////
    //////// advanced search filters
    ////////////////////////////////////////////////////////////////////////////////////////////

    jQuery('#openmap').on( 'click', function(event) {

        if( jQuery(this).find('i').hasClass('fa-angle-down') ){
            jQuery(this).empty().append('<i class="fas fa-angle-up"></i>'+control_vars.close_map);

            if (control_vars.show_adv_search_map_close === 'no') {
                jQuery('.search_wrapper').addClass('adv1_close');
                wpestate_adv_search_click();
            }

        }else{
            jQuery(this).empty().append('<i class="fas fa-angle-down"></i>'+control_vars.open_map);

        }
        wpestate_new_open_close_map(2);

    });


    ///////////////////////////////////////////////////////////////////////////////////////////
    //////// full screen map
    ////////////////////////////////////////////////////////////////////////////////////////////
    var wrap_h;
    var map_h;

    jQuery('#gmap-full').on( 'click', function(event) {


        if(  jQuery('#gmap_wrapper').hasClass('fullmap') ){
            jQuery('#google_map_prop_list_wrapper').removeClass('fullhalf');

            jQuery('#gmap_wrapper').removeClass('fullmap').css('height',wrap_h+'px');
            jQuery('#googleMap').removeClass('fullmap').css('height',map_h+'px');
            jQuery('.master_header ').removeClass('header_full_map');
            jQuery('#search_wrapper').removeClass('fullscreen_search');
            jQuery('#search_wrapper').removeClass('fullscreen_search_open');
            
                if(  !jQuery('#google_map_prop_list_wrapper').length ){
                    jQuery('.content_wrapper').show();
                }
            jQuery('body,html').animate({
                 scrollTop: 0
            }, "slow");
            jQuery('#openmap').show();
            jQuery(this).empty().append('<i class="fas fa-arrows-alt"></i>'+control_vars.fullscreen).removeClass('spanselected');

            jQuery('#google_map_prop_list_wrapper').removeClass('fullscreen');
            jQuery('#google_map_prop_list_sidebar').removeClass('fullscreen');


        }else{
            jQuery('#gmap_wrapper,#googleMap').css('height','100%').addClass('fullmap');

            jQuery('#google_map_prop_list_wrapper').addClass('fullscreen');
            jQuery('#google_map_prop_list_sidebar').addClass('fullscreen');




            jQuery('#google_map_prop_list_wrapper').addClass('fullhalf');


            wrap_h=jQuery('#gmap_wrapper').outerHeight();
            map_h=jQuery('#googleMap').outerHeight();

            jQuery('.master_header ').addClass('header_full_map');


            jQuery('#search_wrapper').addClass('fullscreen_search');
         
            if(  !jQuery('#google_map_prop_list_wrapper').length ){
                jQuery('.content_wrapper').hide();
            }

            jQuery('#openmap').hide();
            jQuery(this).empty().append('<i class="fas fa-square"></i>'+control_vars.default).addClass('spanselected');

        }
        wpresidence_map_resise();



    });


    jQuery('#street-view').on( 'click', function(event) {
         wpestate_toggleStreetView();
    });





    ///////////////////////////////////////////////////////////////////////////////////////////
    ///////     caption-wrapper
    ///////////////////////////////////////////////////////////////////////////////////////////

    jQuery('.caption-wrapper').on( 'click', function(event) {
        jQuery(this).toggleClass('closed');
        jQuery('.carusel-back').toggleClass('rowclosed');
        jQuery('.wpestate_horizontal_slider_wrapper  .carousel-indicators').toggleClass('rowclosed');
    });

    jQuery('.carousel-round-indicators li').on( 'click', function(event) {
        jQuery('.carousel-round-indicators li').removeClass('active');
        jQuery(this).addClass('active');
    });

    jQuery('.videoitem iframe').on( 'click', function(event) {
        jQuery('.estate_video_control').remove();
    });
    ///////////////////////////////////////////////////////////////////////////////////////
    ////// Advanced search
    /////////////////////////////////////////////////////////////////////////////////////////

    wpestate_adv_search_click();


     jQuery('.custom_details_container .send_email_agent').on( 'click', function(event) {
         event.preventDefault();

         jQuery('body,html').animate({
               scrollTop: ( jQuery("#show_contact").offset().top-100)
          }, "slow");

     });

    ///////////////////////////////////////////////////////////////////////////////////////////
    ///////   back to top
    ///////////////////////////////////////////////////////////////////////////////////////////


     jQuery('.backtop').on( 'click', function(event) {
         event.preventDefault();

         jQuery('body,html').animate({
                scrollTop: 0
          }, "slow");

     });

    ///////////////////////////////////////////////////////////////////////////////////////////
    ///////    footer contact
    ///////////////////////////////////////////////////////////////////////////////////////////

    jQuery('.contact-box ').on( 'click', function(event) {
        event.preventDefault();
        jQuery('.contactformwrapper').toggleClass('hidden');
        wpestate_contact_footer_starter();
    });





    ///////////////////////////////////////////////////////////////////////////////////////////
    /////// Search widget
    ///////////////////////////////////////////////////////////////////////////////////////////
    jQuery('#searchform input').focus(function(){
      jQuery(this).val('');
    }).blur(function(){

    });



    ////////////////////////////////////////////////////////////////////////////////////////////
    /// adding total for featured listings
    ///////////////////////////////////////////////////////////////////////////////////////////
    jQuery('.extra_featured').on('change', function(){
       var parent= jQuery(this).parent().parent();
       var price_regular  = parseFloat( parent.find('.submit-price-no').text(),10 );
       var price_featured = parseFloat( parent.find('.submit-price-featured').text(),10 );
       var total= price_regular+price_featured;

       if( jQuery(this).is(':checked') ){
            parent.find('.submit-price-total').text(total);
            parent.find('.stripe_form_featured').show();
            parent.find('.stripe_form_simple').hide();
       }else{
           //substract from total
            parent.find('.submit-price-total').text(price_regular);
            parent.find('.stripe_form_featured').hide();
            parent.find('.stripe_form_simple').show();
       }
    });


    
    /////////////////////////////////////////////////////////////////////////////////////////
    /////// grid to list view 
    ///////////////////////////////////////////////////////////////////////////////////////////


    jQuery('.col-md-12.listing_wrapper .property_unit_custom_element.image').each(function(){
       jQuery(this).parent().addClass('wrap_custom_image');
    });






    ///////////////////////////////////////////////////////////////////////////////////////////
    ///////   compare action
    ///////////////////////////////////////////////////////////////////////////////////////////
    var already_in=[];
    jQuery('#compare_close').on( 'click', function(event) {
        jQuery('.prop-compare').animate({
            right: "-240px"
        });
    });

    jQuery('.compare-action').on( 'click', function(e) {

        e.preventDefault();
        e.stopPropagation();
        jQuery('.prop-compare').animate({
                            right: "0px"
                        });

        var post_id = jQuery(this).attr('data-pid');
         for(var i = 0; i < already_in.length; i++) {
            if(already_in[i] === post_id) {
                return;
            }
        }

        already_in.push(post_id);


        var post_image = jQuery(this).attr('data-pimage');

        var to_add = '<div class="items_compare" style="display:none;"><img src="' + post_image + '"  class="img-responsive"><input type="hidden" value="' + post_id + '" name="selected_id[]" /></div>';
        jQuery('div.items_compare:first-child').css('background', 'red');
        if (parseInt(jQuery('.items_compare').length,10) > 3) {
            jQuery('.items_compare:first').remove();
        }
        jQuery('#submit_compare').before(to_add);

        jQuery('#submit_compare').on( 'click', function(event) {
            jQuery('#form_compare').trigger('submit');
        });

        jQuery('.items_compare').fadeIn(500);
    });

    jQuery('#submit_compare').on( 'click', function(event) {
        jQuery('#form_compare').trigger('submit');
    });



     /////////////////////////////////////////////////////////////////////////////////////////
     ////// form upload
     /////////////////////////////////////////////////////////////////////////////////////////

    jQuery('#form_submit_2,#form_submit_1 ').on( 'click', function(event) {
        var loading_modal;
        window.scrollTo(0, 0);
        loading_modal='<div class="modal fade" id="loadingmodal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true"><div class="modal-dialog"><div class="modal-content"><div class="modal-body listing-submit"><span>'+control_vars.addprop+'</div></div></div></div></div>';

        jQuery('body').append(loading_modal);
        jQuery('#loadingmodal').modal();
    });


    jQuery('#add-new-image').on( 'click', function(event) {
        jQuery('<p><label for="file">New Image:</label><input type="file" name="upload_attachment[]" id="file_featured"></p> ').appendTo('#files_area');
    });



    jQuery('.delete_image').on( 'click', function(event) {
        var image_id=jQuery(this).attr('data-imageid');

        var curent=jQuery('#images_todelete').val();
        if(curent===''){
              curent=image_id;
        }else{
              curent=curent+','+image_id;
        }

        jQuery('#images_todelete').val(curent) ;
        jQuery(this).parent().remove();
   });

     /////////////////////////////////////////////////////////////////////////////////////////
     ////// mouse over map tooltip
     /////////////////////////////////////////////////////////////////////////////////////////

    jQuery('#googleMap').on('mousemove', function(e){
       jQuery('.tooltip').css({'top':e.pageY,'left':e.pageX, 'z-index':'1'});
    });

    setTimeout(function(){  jQuery('.tooltip').fadeOut("fast");},10000);
});

/////////////////////////////////////////////////////
////////////////// END ready


/**
 * Initializes Bootstrap tooltips for all elements with the 'data-bs-toggle="tooltip"' attribute.
 * This function selects all elements that should have tooltips and applies the Bootstrap tooltip
 * functionality to each of them. It should be called after the DOM is fully loaded to ensure all
 * elements are accessible.
 */
function wpestate_initializeTooltips_bootstrap53() {
    // Select all elements with the 'data-bs-toggle="tooltip"' attribute
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));

    // Initialize a Bootstrap tooltip for each selected element
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
      return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}



/*
* control google map types
*
*/

function wpestate_map_types_control(){
    

    jQuery('#map-view').on( 'click', function(event) {
        jQuery('.wpresidence_map_styles_wrapper').toggle();
    });

    jQuery('.map-type').on( 'click', function(event) {
        var map_type;
        jQuery('.wpresidence_map_styles_wrapper').hide();
        map_type=jQuery(this).attr('id');
        wpestate_change_map_type(map_type);

    });

  

}


/*
* adjust sticky footer
*
*/



function wpestate_adjust_sticky_footer(){
    if(control_vars.sticky_footer==='yes'){
        var footer_height=jQuery('#colophon').height();
        jQuery('.wpresidence_main_wrapper_class:not(.has_header_type4)').css('margin-bottom', footer_height);
        jQuery('.wpresidence_wrapper_for_header_4_colum').css('padding-bottom',footer_height);
    }

}


/*
*multiple currency set coookie
*
*/


function wpestate_change_currency_dropdown_action(){
   
    jQuery('.list_sidebar_currency li').on( 'click', function(event) {

        var ajaxurl,data,pos,symbol,coef,curpos, pick;

        data=jQuery(this).attr('data-value');
        pos=jQuery(this).attr('data-pos');
        symbol=jQuery(this).attr('data-symbol');
        coef=jQuery(this).attr('data-coef');
        curpos=jQuery(this).attr('data-curpos');

		var parent_pointer = jQuery(this).parents('.dropdown ');
	
        ajaxurl     =   ajaxcalls_vars.admin_url + 'admin-ajax.php';
        var nonce = jQuery('#wpestate_change_currency').val();
        jQuery.ajax({
            type: 'POST',
            url: ajaxurl,
            data: {
                'action'    :   'wpestate_set_cookie_multiple_curr',
                'curr'      :   data,
                'pos'       :   pos,
                'symbol'    :   symbol,
                'coef'      :   coef,
                'curpos'    :   curpos,
                'security'  :   nonce
            },
            success: function (data) {

                location.reload();
            },
            error: function (errorThrown) {}
        });//end ajax

    });

}

/*
*
*
*
*/




function wpestate_adjustMegaMenuPosition() {
    jQuery(".wpestate_mega_menu2_wrapper").each(function() {
      var $megaMenu = jQuery(this);
      var parentOffset = $megaMenu.closest("li").offset().left;
        var viewportWidth = jQuery(window).width();
      $megaMenu.css({
    
        left: -parentOffset + "px",
        width: viewportWidth + "px",
        "max-width": viewportWidth + "px",
      });
    });
}


/**
 * WpEstate Advanced Filters Multi-Select
 * Handles the chained filtering between County/State -> City -> Area dropdowns
 * Uses Bootstrap Select plugin for enhanced multi-select functionality
 * 
 * HTML Structure Expected:
 * - Multi-select dropdowns with class 'wpestatemultiselectselectpicker'
 * - Options with data-taxonomy (county / state, cities, areas)
 * - Options with data-parent-value matching parent selection values
 * 
 * @since 5.0.5
 */
/**
 * WpEstate Advanced Filters Multi-Select
 * Handles the chained filtering between County/State -> City -> Area dropdowns
 */
function wpestate_advnced_filters_bars() {
    function initializeSelectPicker(element) {
        element.selectpicker({
            styleBase: 'wpestate-multiselect-custom-style',
            size: 7
        }).on('changed.bs.select', function(e, clickedIndex, isSelected, previousValue) {
        
            
            var elememtParent = jQuery(this).closest('.tab-pane.active');
            if (elememtParent.length == 0) {
                elememtParent = jQuery(this).parents().eq(2);
            }
            
            var elementId = jQuery(this).attr('id');
            
            const county_relations = {
                'county-state': 'advanced_city',
                'sidebar-county-state': 'sidebar-advanced_city',
                'shortcode-county-state': 'shortcode-advanced_city',
                'mobile-county-state': 'mobile-advanced_city'
            };
            
            const city_relations = {
                'advanced_city': 'advanced_area',
                'sidebar-advanced_city': 'sidebar-advanced_area',
                'shortcode-advanced_city': 'shortcode-advanced_area',
                'mobile-advanced_city': 'mobile-advanced_area'
            };
            
            var selectedOption = jQuery(this).find('option').eq(clickedIndex);
            var attributeTaxonomy = selectedOption.attr('data-taxonomy');
            var selectedOptions = jQuery(this).find('option:selected');
            
            if (attributeTaxonomy === 'cities' || attributeTaxonomy === 'county / state') {
                triggerChangeOn = county_relations[elementId];
                if (attributeTaxonomy === 'cities') {
                    triggerChangeOn = city_relations[elementId];
                }
                
                var selectedValues = jQuery(this).find('option:selected').map(function() {
                    return jQuery(this).val();
                }).get();
                
                var elementCousin = elememtParent.find('#' + triggerChangeOn);
                elementCousin.find('option').each(function() {
                    var optionValue = jQuery(this).data('parent-value');
                    if (selectedValues.length === 0 || selectedValues.indexOf(optionValue) !== -1) {
                        jQuery(this).show();
                    } else {
                        jQuery(this).hide();
                    }
                });
                
                // Destroy and reinitialize with all event handlers
                elementCousin.selectpicker('destroy');
                initializeSelectPicker(elementCousin);
            }
            
            first_time_wpestate_show_inpage_ajax_half = 1;
            wpestate_show_pins();
            
        }).on('shown.bs.select', function(e) {
            var thisButton = jQuery(this);
            
            var windowWidth = jQuery(window).width();
            var buttonOffset = thisButton.offset();
            var buttonWidth = thisButton.outerWidth();
            var $dropdownMenu = jQuery(this).parent().find('.dropdown-menu.open');
            
            var distanceFromLeft = buttonOffset.left;
            var distanceFromRight = windowWidth - (buttonOffset.left + buttonWidth);
            
            if (distanceFromLeft <= 150) {
                $dropdownMenu.css('left', 0);
                $dropdownMenu.css('right', 'auto');
            } else if (distanceFromRight <= 150) {
                $dropdownMenu.css('left', 'auto');
                $dropdownMenu.css('right', 0);
            }
        });
    }
    
    // Initialize all selectpickers
    jQuery('.wpestatemultiselectselectpicker').each(function() {
        initializeSelectPicker(jQuery(this));
    });
}




/**
 * Function to show/hide hidden fields in the WP Estate search form.
 * This function handles both floating and non-floating search forms,
 * providing smooth animations for expanding and collapsing advanced search options.
 */
function wpestate_show_hide_search_form_hidden_fields(){
    // Variable to store the original top position of the search wrapper
    var handler_top;

    // Attach click event handler to the advanced search toggle button
    jQuery('.adv_handler').on('click', function(event) {
        // Prevent the default action of the click event
        event.preventDefault();

        // Select the hidden fields container
        var check_row = jQuery('.adv_search_hidden_fields');

        // Check if the search form is floating
        if (jQuery('#search_wrapper').hasClass('with_search_form_float')) {
            // Handle floating search form
            if (!jQuery('#search_wrapper').hasClass('openmore')) {
                // If the form is not expanded, prepare to open it

                // Temporarily set display to flex and hide to measure accurate height
                // This is necessary because flex display can affect the element's dimensions
                check_row.css({'display': 'flex', 'visibility': 'hidden'});
                var height = check_row.outerHeight();
                // Reset to original state before animation
                check_row.css({'visibility': '', 'display': 'none'});
               
                // Get the current top position of the search wrapper
                handler_top = parseInt(jQuery('#search_wrapper').css('top'));
                // Calculate the new top position
                
                
               
                // Slide down hidden fields and set to flex display after animation
                // This ensures smooth animation while achieving the desired flex layout
                check_row.slideDown({
                    duration: 200,
                    queue: false,
                    complete: function() {
                        jQuery(this).css('display', 'flex');
                    }
                });
               
                // Mark the search wrapper as expanded
                jQuery('#search_wrapper').addClass('openmore');
            } else {
                // If the form is already expanded, close it

                // Animate the search wrapper back to its original position
                jQuery('#search_wrapper').animate({
                    'top': handler_top
                }, { duration: 200, queue: false });
               
                // Slide up the hidden fields
                check_row.slideUp({ duration: 200, queue: false });
                // Remove the expanded state class
                jQuery('#search_wrapper').removeClass('openmore');
            }
        } else {
            // Handle non-floating search form
            if (check_row.css('display') === 'none') {
                // If hidden fields are not visible, show them
                // Set display to flex, hide it, then slide down for smooth animation
                check_row.css('display', 'flex').hide().slideDown(200);
            } else {
                // If hidden fields are visible, hide them
                check_row.slideUp(200);
            }
        }
    });
}




/**
 * Function to control "more options" in search form
 * 
 * This function sets up event handlers for expanding and collapsing
 * additional search options in  real estate search form.
 * 
 * 
 */
function wpestate_extended_search_function(){
    // Event handler for expanding the search options
    jQuery('.residence_adv_extended_options_text').on('click', function(event) {
        var parent_wrapper = jQuery(this).parent();
        
        // Hide the "More options" text
        jQuery(this).hide();
        
        // Find and show the extended search options
        var check_row = parent_wrapper.find('.extended_search_check_wrapper');
        check_row.slideDown({ duration: 200, queue: false });
        
        // Show the close button
        parent_wrapper.find('.adv_extended_close_button').show();
    });

    // Event handler for collapsing the search options
    jQuery('.adv_extended_close_button').on('click', function(event) {
        var parent_wrapper = jQuery(this).parent().parent();
        
        // Hide the close button
        jQuery(this).hide();
        
        // Show the "More options" text
        parent_wrapper.find('.residence_adv_extended_options_text').show();
        
        // Hide the extended search options
        parent_wrapper.find('.extended_search_check_wrapper').slideUp({ duration: 200, queue: false });
    });
}



/*
 * wp_estate_baths_component_item
 *
 * This function sets up the behavior for a beds and baths selection component
 * in a real estate WordPress theme or plugin.
 */
function wpestate_beds_baths_component() {
   
    /**
     * Handles the click event on beds or baths items
     * @param {string} itemClass - The class of the clicked item
     * @param {string} inputClass - The class of the input to update
     */
    function handleItemClick(itemClass, inputClass) {
        var selected_item = jQuery(this);
        var parent = selected_item.parent();
        var component = selected_item.closest('.wpestate-beds-baths-popoup-component');
        // Remove selected class from all items and add it to the clicked item
        parent.find(itemClass).removeClass('wp_estate_component_item_selected');
        selected_item.addClass('wp_estate_component_item_selected');
        // Update the hidden input value
        var selected_value = selected_item.attr('data-value');
        jQuery(inputClass).val(selected_value);
        update_label(component);
    }

    /**
     * Updates the label of the component with the selected beds and baths
     * @param {jQuery} component - The component to update
     */
    function update_label(component){
        var selected_beds = component.find('.wp_estate_beds_component_item.wp_estate_component_item_selected').length > 0 ? component.find('.wp_estate_beds_component_item.wp_estate_component_item_selected').attr('data-value') : '0';
        var selected_baths = component.find('.wp_estate_baths_component_item.wp_estate_component_item_selected').length > 0 ? component.find('.wp_estate_baths_component_item.wp_estate_component_item_selected').attr('data-value') : '0';
        var update_label=parseFloat(selected_beds)+'+ '+control_vars.bd+'/'+parseFloat(selected_baths)+'+ '+control_vars.ba;
        component.find('.dropdown-toggle').text(update_label);
    }

    // Attach event handlers
    // Handler for bed selection
    jQuery('.wp_estate_beds_component_item').on('click', function(event) {
        event.stopPropagation();
        handleItemClick.call(this, '.wp_estate_beds_component_item', '.wpresidence-componentsbeds');
    });

    // Handler for bath selection
    jQuery('.wp_estate_baths_component_item').on('click', function(event) {
        event.stopPropagation();
        handleItemClick.call(this, '.wp_estate_baths_component_item', '.wpresidence-componentsbaths');
    });

    // Handler for resetting the selection
    jQuery('.wpestate-beds-baths-popoup-reset').on('click', function(event) {
        event.stopPropagation();
        var parent = jQuery(this).closest('.wpestate-beds-baths-popoup-wrapper');
        // Remove selected class from all items
        parent.find('.wp_estate_baths_component_item').removeClass('wp_estate_component_item_selected');
        parent.find('.wp_estate_beds_component_item').removeClass('wp_estate_component_item_selected');
        // Clear input values
        parent.find('.wpresidence-componentsbeds, .wpresidence-componentsbaths').val('');
        // Reset the label
        var component = jQuery(this).closest('.wpestate-beds-baths-popoup-component');
        var update_value=jQuery(this).attr('data-default-value')+'<span class="caret caret_filter "></span>';
        component.find('.dropdown-toggle').html(update_value);
    });

    // Handler for completing the selection
    jQuery('.wpestate-beds-baths-popoup-done').on('click', function() {
        first_time_wpestate_show_inpage_ajax_half=1;
        wpestate_show_pins();
    });
 
}


/*
 * wp_estate_price_component_item
 *
 * This function sets up the behavior for a price range selection component
 * in a real estate WordPress theme or plugin.
 */
function wpestate_price_component_item(){

    // Prevent click event propagation on the price popup wrapper
    jQuery('.wpestate-price-popoup-wrapper').on('click', function(event) {
        event.stopPropagation();
    });

    // Clear input value on focus for price range inputs
    jQuery('.component_adv_search_elementor_price_low,.component_adv_search_elementor_price_max').on('focus',function(){
        jQuery(this).val('');
    })

    // Ensure only numeric input for price range fields
    jQuery('.component_adv_search_elementor_price_low,.component_adv_search_elementor_price_max').on('input', function() {
        // Replace non-digit characters with an empty string
        jQuery(this).val(jQuery(this).val().replace(/\D/g, ''));
    });

    // Update slider when price input changes
    jQuery('.component_adv_search_elementor_price_low,.component_adv_search_elementor_price_max').on('change',function(event) {
        var inputValue = jQuery(this).val();
        var parent = jQuery(this).closest('.wpestate-price-popoup-wrapper');
        var numericValue = parseFloat(inputValue)
        var slider = parent.find('.ui-slider')
        
        // Update slider based on which input changed (low or max)
        if( jQuery(this).hasClass('component_adv_search_elementor_price_low') ){      
            slider.slider('values', 0, numericValue);
            slider.slider("option", "slide").call(slider, null, { values: [numericValue, slider.slider("values")[1]] });
        }else{
            slider.slider('values', 1, numericValue);
            slider.slider("option", "slide").call(slider, null, { values: [slider.slider("values")[0], numericValue] });
        }
    });

    // Reset price component to default values
    jQuery('.wpestate-price-component-popoup-reset').on('click',function(event){
        event.stopPropagation();
        var parent = jQuery(this).closest('.wpestate-price-popoup-wrapper');
        var low_value_label = parent.find('.wpestate-price-popoup-field-low').attr('data-value');
        var max_value_label = parent.find('.wpestate-price-popoup-field-max').attr('data-value');
       
        // Reset values based on the type of price input present
        if(parent.find('.adv_search_elementor_price_low').length>0 ){
            var low_value = parent.find('.adv_search_elementor_price_low').attr('data-value');
            var max_value = parent.find('.adv_search_elementor_price_max').attr('data-value');
            parent.find('.adv_search_elementor_price_low').val(low_value);
            parent.find('.adv_search_elementor_price_max').val(max_value);
        }else{
            if( parent.find('.adv6_price_low').length > 0 ){
                var low_value = parent.find('.adv6_price_low').attr('data-value');
                var max_value = parent.find('.adv6_price_max').attr('data-value');
                parent.find('.adv6_price_low').val(low_value);
                parent.find('.adv6_price_max').val(max_value);
            }else{
                var low_value = parent.find('.single_price_low').attr('data-value');
                var max_value = parent.find('.single_price_max').attr('data-value');
                parent.find('.adv6_price_low').val(low_value);
                parent.find('.adv6_price_max').val(max_value);
            }
        }

        // Reset dropdown button text
        var button_item = parent.parent().find('.dropdown-toggle');
        button_item.html( button_item.attr('data-default-value')+' <span class="caret caret_filter "></span>' ) ;
        
        // Reset price range fields
        parent.find('.wpestate-price-popoup-field-low').val(low_value_label);
        parent.find('.wpestate-price-popoup-field-max').val(max_value_label);
   
        // Reset slider label and values
        var slider_label = parent.find('.wpresidence_slider_price').attr('data-default');
        parent.find('.wpresidence_slider_price').text(slider_label);
        var slider = parent.find('.ui-slider')
        slider.slider('values', 0, parseFloat(low_value)); // sets first handle (e.g., min value)
        slider.slider('values', 1, parseFloat(max_value));
    })

    // Handle 'Done' button click
    jQuery('.wpestate-price-component-popoup-done').on('click',function(event){
        var parent_wrapper = jQuery('body');
        var fake_click_item = parent_wrapper.parent();
        fake_click_item.trigger('click');
        wpestate_show_pins();
    })
}



/*
*
* format a price
*
*/
function wpestate_format_a_price(priceValue){
    thousandSeparator   = control_vars.price_separator;
    decimalPoints       = control_vars.decimal_poins;
    decimalSeparator    = control_vars.decimal_poins_separator;

    if (!isNaN(priceValue)) {
        priceValue= parseFloat(priceValue);
        var formattedPrice = priceValue.toFixed(decimalPoints).replace(/\d(?=(\d{3})+\.)/g, '$&' + thousandSeparator);
        formattedPrice = formattedPrice.replace('.', decimalSeparator);
        var formattedPriceWithCurrency= wpestate_return_price_with_multi_currency(formattedPrice);
        return formattedPriceWithCurrency;
    }


}


/*
*
* add currency to a price
*
*/

function wpestate_return_price_with_multi_currency(theValue){
    var return_string='';
    var my_custom_curr_symbol  =   decodeURI ( wpestate_getCookie('my_custom_curr_symbol') );
    var my_custom_curr_coef    =   parseFloat( wpestate_getCookie('my_custom_curr_coef'));
    var my_custom_curr_pos     =   parseFloat( wpestate_getCookie('my_custom_curr_pos'));
    var my_custom_curr_cur_post=   wpestate_getCookie('my_custom_curr_cur_post');

   

    if (!isNaN(my_custom_curr_pos) && my_custom_curr_pos !== -1) {
        if (my_custom_curr_cur_post === 'before') {
            return_string= wpestate_replace_plus( decodeURIComponent ( my_custom_curr_symbol ) ) + " " + theValue;
        } else {
            return_string=theValue + " " +  wpestate_replace_plus( decodeURIComponent ( my_custom_curr_symbol ) ) ;
        }
    } else {

        if (control_vars.where_curency === 'before') {      
            return_string= wpestate_replace_plus( decodeURIComponent ( control_vars.curency ) ) + " " + theValue;
        } else {     
            return_string=theValue + " " +  wpestate_replace_plus( decodeURIComponent ( control_vars.curency ) ) ;
        }
    }

    return return_string;



}
/*
*
* wp estate price component_item v3
*
*/
function wpestate_price_component_item_v3(){
    jQuery('.wpestate-price-popoup-wrapper_v3').on('click', function(event) {

        event.stopPropagation();
        jQuery(this).find('.dropdown-menu').hide();  
    });

      
    jQuery('.wpestate_child_dropdown_item').on("click", function(e) {
        jQuery(this).parent().find('.dropdown-menu').toggle();  
        e.stopPropagation();
    });

   
    jQuery('.wpestate-price-component-popoup-reset_v3').on('click',function(event){
        event.stopPropagation();
        var parent = jQuery(this).closest('.wpestate-price-popoup-wrapper_v3');
    
        parent.find('.wpestate-price-component-select').each(function() {
            jQuery(this).val(jQuery(this).find('option:first').val());
        });

        var component = parent.closest('.wpestate-beds-baths-popoup-component');
        var update_value=jQuery(this).attr('data-default-value')+'<span class="caret caret_filter"></span>';

        var item_update_value = jQuery(this).attr('data-default-value2')+'<span class="caret caret_filter"></span>';
        component.find('.dropdown-toggle').html(update_value);
        component.find('.wpestate_child_dropdown_item').html(item_update_value);
        component.find('.wpresidence-component3_input_class').val('');
        component.find('.price_label_component').val('');

    })

    jQuery('.wpestate-price-component-popoup-done_v3').on('click',function(event){
        var parent_wrapper = jQuery('body');
        var fake_click_item= parent_wrapper.parent();
        fake_click_item.trigger('click');
        first_time_wpestate_show_inpage_ajax_half=1;
        wpestate_show_pins();
    })
 
 
    jQuery('.wpestate-price-popoup-wrapper_v3 li').on("click", function(e){      
        
        var parent = jQuery(this).closest('.wpestate-price-popoup-wrapper_v3');
        var component = parent.closest('.wpestate-beds-baths-popoup-component');
        var parent_ul = jQuery(this).parent();
        var selected_value = jQuery(this).attr('data-value');

        if(parent_ul.hasClass('wpresidence-component3-min-price_class')){
            var selectedmin =selected_value;
            var selectedmax = parent.find('.wpresidence-component3-max-price_input_class').val();
        }else{
            var selectedmin = parent.find('.wpresidence-component3-min-price_input_class').val();
            var selectedmax = selected_value
        }


        var update_value=wpestate_return_price_with_multi_currency(selectedmin)+" - "+wpestate_return_price_with_multi_currency(selectedmax);
        component.find('.wpestate-beds-baths-popoup-component-toggle').html(update_value);
        component.find('.price_label_component').val(update_value);        
        parent_ul.hide();
    });

}




/*
*
*wpestate-price-popoup-wrapper_v3
*/
function wpestate_reposion_dropdowns(){
    jQuery('.wpestate-multiselect-custom-style').on('click', function() {
       
    var $popupWrapper = jQuery(this);
    var $searchWrapper = $popupWrapper.closest('.search_wrapper'); // Get the closest parent with the class '.search_wrapper'

    if($searchWrapper.length===0){
        return;
    }

    var $dropdownMenu = jQuery(this).next('.wpestate-price-popoup-wrapper_v3');

    if($dropdownMenu.length==0){
        var $dropdownMenu = jQuery(this).next('.wpestate-beds-baths-popoup-wrapper');
    }
    if($dropdownMenu.length==0){
        var $dropdownMenu = jQuery(this).next('.wpestate-price-popoup-wrapper');
    }

    var popupOffset = $popupWrapper.offset();
    var searchOffset = $searchWrapper.offset();

    // Calculate the distances
    var distanceToLeft = popupOffset.left - searchOffset.left;
    var distanceToRight = (searchOffset.left + $searchWrapper.outerWidth()) - (popupOffset.left + $popupWrapper.outerWidth());

    if (distanceToLeft <= 150) {
        // Close to the left edge
        $dropdownMenu.css('left', 0);
        $dropdownMenu.css('right', 'auto');

    } else if (distanceToRight <= 150) {
        // Close to the right edge
        $dropdownMenu.css('left', 'auto');
        $dropdownMenu.css('right', 0);

    } 

        
        

    });
}


/**
* Initializes Slick Slider for Estate Place Cards
* @param {Object} jQuery - Requires jQuery
* @requires slick-carousel
* @description Sets up responsive sliders for real estate location/place displays with optional autoplay
* Data attributes required:
* - data-items-per-row: Number of slides to show
* - data-auto: Autoplay speed in ms (0 to disable)
* Global required:
* - control_vars.is_rtl: RTL language setting ('1' or '0')
*/

function wpestate_enable_slick_places() {
    jQuery('.estate_places_slider').not('.slick-initialized').each(function() {
        let items = jQuery(this).attr('data-items-per-row'),
            auto = parseInt(jQuery(this).attr('data-auto')),
            isRTL = control_vars.is_rtl === '1';
 
        const config = {
            infinite: true,
            slidesToShow: items,
            slidesToScroll: 1,
            dots: false,
            rtl: isRTL,
            responsive: [
                {
                    breakpoint: 1025,
                    settings: {
                        slidesToShow: 2,
                        slidesToScroll: 1
                    }
                },
                {
                    breakpoint: 525, 
                    settings: {
                        slidesToShow: 1,
                        slidesToScroll: 1
                    }
                }
            ]
        };
 
        if (auto) {
            config.autoplay = true;
            config.autoplaySpeed = auto;
        }
 
        jQuery(this).slick(config);
    });
 }


/*
*
*
*/

function wpestate_enble_slick_slider_list(){
    jQuery('.shortcode_slider_list').each(function() {
        const $slider = jQuery(this);
        const items = $slider.attr('data-items-per-row');
        const auto = parseInt($slider.attr('data-auto'));
        const isRTL = control_vars.is_rtl === '1';
        
        const slickConfig = {
            infinite: true,
            slidesToShow: items,
            slidesToScroll: 1,
            dots: true,
            rtl: isRTL,
            autoplay: auto !== 0,
            autoplaySpeed: auto,
            responsive: [
                {
                    breakpoint: 1025,
                    settings: {
                        slidesToShow: 2,
                        slidesToScroll: 1
                    }
                },
                {
                    breakpoint: 550,
                    settings: {
                        slidesToShow: 1,
                        slidesToScroll: 1
                    }
                }
            ]
        };
        
        // Initialize slick with settings
        $slider.not('.slick-initialized').slick(slickConfig);
    });
}

/**
 * Initialize and configure Slick slider for theme and property image sliders
 * Handles both RTL and LTR layouts with responsive design and mobile optimization
 * 
 * @param {number|string} items_received - Number of slides to show on desktop view (defaults to 3 if invalid)
 * 
 * Configuration:
 * - Supports auto-play and manual navigation modes
 * - Responsive breakpoints at 1025px (2 slides) and 576px (1 slide)
 * - RTL support with mobile positioning fixes
 * - Custom navigation arrows with Bootstrap carousel styling
 * - Adaptive height on mobile devices
 * 
 * HTML Requirements:
 * - Target elements: .theme_slider_2 or .property_multi_image_slider
 * - data-auto attribute for autoplay speed (0 for manual navigation)
 * 
 * Dependencies:
 * - jQuery
 * - Slick Slider
 * - control_vars.is_rtl global variable ('1' for RTL, '0' for LTR)
 * 
 * @return {void}
 */
function wpestate_enable_slick_theme_slider(items_received) {
    var items = 3;
    if (parseInt(items_received) !== '') {
        items = parseInt(items_received);
    }

    jQuery('.theme_slider_2,.property_multi_image_slider').each(function() {
        var auto = parseInt(jQuery(this).attr('data-auto'));
        var isRTL = control_vars.is_rtl === '1';
        
        // Common settings for both auto and non-auto
        var commonSettings = {
            infinite: true,
            slidesToShow: items,
            slidesToScroll: 1,
            dots: auto === 0 ? false : true,
            rtl: isRTL,
            prevArrow: '<button class="carousel-control-prev wpresidence-carousel-control" type="button" data-bs-target="#wpresidence-blog-post-carousel-bootstrap" data-bs-slide="prev"><i class="demo-icon icon-left-open-big"></i><span class="visually-hidden">Previous</span></button>',
            nextArrow: '<button class="carousel-control-next wpresidence-carousel-control" type="button" data-bs-target="#wpresidence-blog-post-carousel-bootstrap" data-bs-slide="next"><i class="demo-icon icon-right-open-big"></i><span class="visually-hidden">Next</span></button>',
            responsive: [
                {
                    breakpoint: 1025,
                    settings: {
                        slidesToShow: 2,
                        slidesToScroll: 1
                    }
                },
                {
                    breakpoint: 576,
                    settings: {
                        slidesToShow: 1,
                        slidesToScroll: 1,
                        adaptiveHeight: true
                    }
                }
            ],
            init: function(slick) {
                if (isRTL) {
                    // Fix for initial RTL positioning
                    setTimeout(function() {
                        var $track = jQuery(slick.$slideTrack);
                        var width = $track.width();
                        var slideWidth = jQuery(window).width();
                        
                        if (window.innerWidth <= 576) {
                            $track.css({
                                'transform': 'translate3d(0, 0, 0)',
                                'width': 'auto'
                            });
                        }
                    }, 100);
                }
            }
        };

        // Add auto-specific settings
        if (auto !== 0) {
            commonSettings.autoplay = true;
            commonSettings.autoplaySpeed = auto;
        }

        // Initialize slick if not already initialized
        if (!jQuery(this).hasClass('slick-initialized')) {
            jQuery(this).slick(commonSettings);
        }

        // Handle window resize
        jQuery(window).on('resize', function() {
            if (isRTL && window.innerWidth <= 576) {
                jQuery('.slick-track').css({
                    'transform': 'translate3d(0, 0, 0)',
                    'width': 'auto'
                });
            }
        });
    });
}


/*
*
*
*/
function wpestate_enable_slick_testimonial(){
    jQuery('.testimonial-slider-container').each(function(){


        var items   = parseInt( jQuery(this).attr('data-visible-items'));
        var auto    = parseInt( jQuery(this).attr('data-auto') );

        if (auto === 0 ){

            jQuery(this).not('.slick-initialized').slick({
                infinite: true,
                slidesToShow: items,
                slidesToScroll: 1,
                dots: true,

                responsive: [
                    {
                        breakpoint:1025,
                        settings: {
                          slidesToShow: 1,
                          slidesToScroll: 1
                        }
                    },
                    {
                      breakpoint: 480,
                      settings: {
                        slidesToShow: 1,
                        slidesToScroll: 1
                      }
                    }
                ]
            });
            if(control_vars.is_rtl==='1'){
                  jQuery(this).slick('slickSetOption','rtl',true,true);
            }
        }else{

            jQuery(this).not('.slick-initialized').slick({
                infinite: true,
                slidesToShow: items,
                slidesToScroll: 1,
                dots: true,
                autoplay: true,
                autoplaySpeed: auto,

                 responsive: [
                    {
                     breakpoint:1025,
                     settings: {
                       slidesToShow: 1,
                       slidesToScroll: 1
                     }
                   },
                    {
                      breakpoint: 480,
                      settings: {
                        slidesToShow: 1,
                        slidesToScroll: 1
                      }
                    }
                ]
            });
            if(control_vars.is_rtl==='1'){
                jQuery(this).slick('slickSetOption','rtl',true,true);
            }
        }
    });
}

/*
*
*
*/
function wpestate_grid_list_controls(){

    jQuery('#list_view').unbind('click');
    jQuery('#list_view').on( 'click', function(event) {
        jQuery(this).toggleClass('icon_selected');
        jQuery('#listing_ajax_container').addClass('ajax12');
        jQuery('#grid_view').toggleClass('icon_selected');


        jQuery('#listing_ajax_container .listing_wrapper,.wpestate_filter_list_properties_wrapper .listing_wrapper').hide().removeClass('col-md-4').removeClass('col-md-6').removeClass('col-md-3').addClass('col-md-12 col-lg-12').fadeIn(400) ;

        jQuery('.the_grid_view').fadeOut(10,function() {
            jQuery('.the_list_view:not(.half_map_list_view)').fadeIn(300);
        });

        // custom unit code
        jQuery('#listing_ajax_container .col-md-12.listing_wrapper .property_unit_custom_element.image').each(function(){
            jQuery(this).parent().addClass('wrap_custom_image');
        });

        jQuery('.col-md-12.listing_wrapper .property_unit_custom_element.image').each(function(){
            jQuery(this).parent().addClass('wrap_custom_image');
        });

        wpresidence_list_view_arrange();
     });



    jQuery('#grid_view').unbind('click');
    jQuery('#grid_view').on( 'click', function(event) {
        var class_type;

        class_type = jQuery('#listing_ajax_container .listing_wrapper').first().attr('data-org');
        if( jQuery('.wpestate_filter_list_properties_wrapper').length >0 ){
            class_type = jQuery('.wpestate_filter_list_properties_wrapper .listing_wrapper').first().attr('data-org');
        }

        jQuery(this).toggleClass('icon_selected');
        jQuery('#listing_ajax_container').removeClass('ajax12');
        jQuery('#list_view').toggleClass('icon_selected');
        jQuery('#listing_ajax_container .listing_wrapper,.wpestate_filter_list_properties_wrapper .listing_wrapper ').hide().removeClass('col-lg-12').removeClass('col-md-12').addClass('col-lg-'+class_type).fadeIn(400);
        jQuery('#listing_ajax_container .the_list_view,.wpestate_filter_list_properties_wrapper .the_list_view').fadeOut(10,function(){
             jQuery('.the_grid_view').fadeIn(300);
        });

        jQuery('#listing_ajax_container .wrap_custom_image').each(function(){
            jQuery(this).removeClass('wrap_custom_image');
            jQuery('.property_listing_custom_design').css('padding-left','0px');
        });

     });



}


/*
*
*
*/
function  wpresidence_list_view_arrange(){
        var wrap_image = parseInt( jQuery('.wrap_custom_image').width());

        if(wrap_image!=0){
           jQuery('.col-md-12>.property_listing_custom_design').css('padding-left',wrap_image);
        }
}



/**
 * Handles the dynamic filtering between city and area dropdowns in property listings.
 * When a city is selected, it filters the available areas to show only those belonging
 * to the selected city. The function works with multiple instances of city-area filter
 * pairs on the same page.
 *
 * @param {string} selected_city - ID of the city dropdown button (without '#')
 * @param {string} selected_area - ID of the area dropdown button (without '#')
 *
 * HTML Structure Expected:
 * <button id="[selected_city]">
 * <ul class="dropdown-menu">
 *   <li data-value2="city-value" data-value="city-value">City Name</li>
 * </ul>
 * <button id="[selected_area]">
 * <ul class="dropdown-menu">
 *   <li data-parentcity="city-value" data-value="area-value">Area Name</li>
 * </ul>
 */
function wpestate_filter_city_area(selected_city, selected_area) {
    // Bind click event to city dropdown items
    jQuery('#' + selected_city).siblings('.dropdown-menu').find('li').on('click', function(event) {
        event.preventDefault();
        
        // Get the selected city value and convert to lowercase for comparison
        var value_city = String(jQuery(this).attr('data-value2')).toLowerCase();
        
        // Update city button text and data value
        var cityText = jQuery(this).text();
        var cityButton = jQuery('#' + selected_city);
        cityButton.text(cityText);
        cityButton.attr('data-value', value_city);
        
        // Filter area options based on selected city
        jQuery('#' + selected_area).siblings('.dropdown-menu').find('li').each(function() {
            var is_city = String(jQuery(this).attr('data-parentcity')).toLowerCase();
            is_city = is_city.replace(" ", "-");
            var area_value = String(jQuery(this).attr('data-value')).toLowerCase();
            
            // Show areas matching selected city or all areas if 'all' is selected
            if (is_city === value_city || value_city === 'all') {
                jQuery(this).show();
            } else {
                jQuery(this).hide();
            }
        });
        
        // Reset area dropdown to default state
        var areaButton = jQuery('#' + selected_area);
        areaButton.text(control_vars.areas);
        areaButton.attr('data-value', 'all');
    });
    
    // Bind click event to area dropdown items
    jQuery('#' + selected_area).siblings('.dropdown-menu').find('li').on('click', function(event) {
        event.preventDefault();
        
        // Update area button text and data value when area is selected
        var areaText = jQuery(this).text();
        var areaValue = jQuery(this).attr('data-value');
        var areaButton = jQuery('#' + selected_area);
        areaButton.text(areaText);
        areaButton.attr('data-value', areaValue);
    });
}


/**
 * Handles the dynamic filtering between county and city dropdowns in property listings.
 * When a county is selected, it filters the available cities to show only those belonging
 * to the selected county. The function works with multiple instances of county-city filter
 * pairs on the same page.
 *
 * @param {string} selected_county - ID of the county dropdown button (without '#')
 * @param {string} selected_city - ID of the city dropdown button (without '#')
 *
 * HTML Structure Expected:
 * <button id="[selected_county]">
 * <ul class="dropdown-menu">
 *   <li data-value2="county-value" data-value="county-value">County Name</li>
 * </ul>
 * <button id="[selected_city]">
 * <ul class="dropdown-menu">
 *   <li data-parentcounty="county-value" data-value="city-value">City Name</li>
 * </ul>
 */
function wpestate_filter_county_city(selected_county, selected_city) {
    // Bind click event to county dropdown items using correct sibling selector
    jQuery('#' + selected_county).siblings('.dropdown-menu').find('li').on('click', function(event) {
        event.preventDefault();
        
        // Get the selected county value and convert to lowercase for comparison
        var value_county = String(jQuery(this).attr('data-value2')).toLowerCase();
        
        // Update county button text and data value
        var countyText = jQuery(this).text();
        var countyButton = jQuery('#' + selected_county);
        countyButton.text(countyText);
        countyButton.attr('data-value', value_county);
        
        // Filter city options based on selected county
        jQuery('#' + selected_city).siblings('.dropdown-menu').find('li').each(function() {
            var is_county = String(jQuery(this).attr('data-parentcounty')).toLowerCase();
            is_county = is_county.replace(" ", "-");
            var city_value = String(jQuery(this).attr('data-value')).toLowerCase();
            
            // Show cities matching selected county or all cities if 'all' is selected
            if (is_county === value_county || value_county === 'all') {
                jQuery(this).show();
            } else {
                jQuery(this).hide();
            }
        });
        
        // Reset city dropdown to default state
        var cityButton = jQuery('#' + selected_city);
        cityButton.text(control_vars.cities);
        cityButton.attr('data-value', 'all');
    });
    
    // Bind click event to city dropdown items
    jQuery('#' + selected_city).siblings('.dropdown-menu').find('li').on('click', function(event) {
        event.preventDefault();
        
        // Update city button text and data value when city is selected
        var cityText = jQuery(this).text();
        var cityValue = jQuery(this).attr('data-value');
        var cityButton = jQuery('#' + selected_city);
        cityButton.text(cityText);
        cityButton.attr('data-value', cityValue);
    });
}




/*
*
*
*/
function wpestate_raisePower(x, y) {
    "use strict";
    return Math.pow(x, y);
}

/*
*
*
*/
function wpestate_adv_search_click(){
    "use strict";    
   jQuery('.with_search_form_float .adv-search-header-1,.with_search_form_float #adv-search-header-3').on( 'click', function(event) {
        if ( jQuery('#search_wrapper').hasClass('float_search_closed') ){
            jQuery('#search_wrapper').removeClass('float_search_closed');
        }else{
            jQuery('#search_wrapper').addClass('float_search_closed');
        }

   });

}


/*
*Contact footer
*
*/
function wpestate_contact_footer_starter(){
    "use strict";
    jQuery('#btn-cont-submit').on( 'click', function(event) {
       
        var contact_name, contact_email, contact_phone, contact_coment, agent_email, property_id, nonce, ajaxurl;
        contact_name    =   jQuery('#foot_contact_name').val();
        contact_email   =   jQuery('#foot_contact_email').val();
        contact_phone   =   jQuery('#foot_contact_phone').val();
        contact_coment  =   jQuery('#foot_contact_content').val();
        nonce           =   jQuery('#agent_property_ajax_nonce').val();
        ajaxurl         =   ajaxcalls_vars.admin_url + 'admin-ajax.php';
        var parent = jQuery(this).parent().parent();

       
        if(ajaxcalls_vars.use_gdpr==='yes'){
            if (!parent.find('.wpestate_agree_gdpr').is(':checked')) {
                jQuery('#footer_alert-agent-contact').empty().append(ajaxcalls_vars.gdpr_terms);
                return;
            }
        }
     
        jQuery('#footer_alert-agent-contact').empty().append(ajaxcalls_vars.sending);



        jQuery.ajax({
            type: 'POST',
            dataType: 'json',
            url: ajaxurl,
            data: {
                'action'    :   'wpestate_ajax_agent_contact_form',
                'name'      :   contact_name,
                'email'     :   contact_email,
                'phone'     :   contact_phone,
                'comment'   :   contact_coment,
                'is_footer' :   'yes',
                'nonce'     :   nonce
            },
            success: function (data) {
       
                if (data.sent) {
                    jQuery('#foot_contact_name').val('');
                    jQuery('#foot_contact_email').val('');
                    jQuery('#foot_contact_phone').val('');
                    jQuery('#foot_contact_content').val('');
                    jQuery('#footer_alert-agent-contact').empty().addClass('wpestate-agent-contact-sent').append(data.response);
                }else{
                    jQuery('#footer_alert-agent-contact').empty().removeClass('wpestate-agent-contact-sent').append(data.response);
                }
   
              
            },
            error: function (errorThrown) {

            }
        });


        jQuery.ajax({
            type: 'POST',
            dataType: 'json',
            url: ajaxurl,
            data: {
                'action'    :   'wpestate_ajax_agent_contact_form_forcrm',
                'name'      :   contact_name,
                'email'     :   contact_email,
                'phone'     :   contact_phone,
                'comment'   :   contact_coment,
                'nonce'     :   nonce
            },
            success: function (data) {


            },
            error: function (errorThrown) {

            }
        });


    });
}


/*
*
*
*/
function estate_splash_slider(){
    if(jQuery("#splash_slider_wrapper").length>0){

    }
}


/*
*
*
*/
function wpestat_resize_wpestate_property_slider_v2(){
    if(control_vars.is_rtl==='1'){
    
    }
    var width   = 1090;
    var window  = jQuery( document ).width();
    
    var toadd=width+(window-width)/2;

    if(toadd>1090){
        jQuery('.property_slider_carousel_elementor_v2').css('width',toadd+'px');
    }else{
        jQuery('.property_slider_carousel_elementor_v2').css('width','100%');
    }
}


/*
*
*
*/
function wpestate_hotspots_click(){
    jQuery('.wpestate_hotspot_icon_wrapper').on('click',function(){
        jQuery('.wpestate_hotspot_tooltip').hide();
        jQuery(this).parent().find('.wpestate_hotspot_tooltip').show();
    });
   
}


/*
*
*
*/
function wpestate_hotspots_hover(){
      jQuery(".wpestate_hotspot_icon_wrapper").on('mouseenter', function () {
        jQuery(this).parent().find('.wpestate_hotspot_tooltip').show();
    }).on('mouseleave', function () {
        jQuery(this).parent().find('.wpestate_hotspot_tooltip').hide();
    });
            
           
}

/**
 * Initialize testimonial slider with RTL support
 * @param {string} slider_id - The ID of the slider element
 */
function wpestate_testimonial_slider(slider_id){

    let rtl_value = false;
    if(control_vars.is_rtl==='1'){
        rtl_value=true;
    }
    jQuery('#'+slider_id).owlCarousel({
        loop:true,
        margin:0,
        nav:true,
        items:1,
        dots:true,
        mouseDrag:true,
        video:true,
        autoHeight: true,
        stagePadding:0,
        URLhashListener:false,
        rtl:rtl_value,
        navText: [
            // Swap arrows for RTL
            rtl_value ? 
                '<i class="fas fa-arrow-right"></i>' : 
                '<i class="fas fa-arrow-left"></i>',
            rtl_value ? 
                '<i class="fas fa-arrow-left"></i>' : 
                '<i class="fas fa-arrow-right"></i>'
        ],
    });
  
}

/*
*
*
*/
function wpestate_property_slider_v2(slider_id, items) {
    wpestat_resize_wpestate_property_slider_v2();
    
    // Initialize RTL value checking both data attribute and control_vars
    let rtl_value = false;
    const $slider = jQuery('#' + slider_id);
    
    // Check both data-rtl and control_vars.is_rtl
    if ($slider.attr('data-rtl') === 'yes' || control_vars.is_rtl === '1') {
        rtl_value = true;
        $slider.addClass('owl-rtl');
    }

    const is_loop = true;

    $slider.owlCarousel({
        loop: is_loop,
        margin: 0,
        nav: true,
        items: 6,
        dots: false,
        mouseDrag: true,
        video: true,
        autoHeight: true,
        autoWidth: true,
        stagePadding: 0,
        URLhashListener: false,
        rtl: rtl_value,
        navText: [
            // Swap arrows for RTL
            rtl_value ? 
                '<i class="fas fa-arrow-right"></i>' : 
                '<i class="fas fa-arrow-left"></i>',
            rtl_value ? 
                '<i class="fas fa-arrow-left"></i>' : 
                '<i class="fas fa-arrow-right"></i>'
        ],
        onInitialized: function() {
            // Force RTL stage direction
            if (rtl_value) {
                const stage = $slider.find('.owl-stage');
                stage.css('transform-origin', 'center center');
                // Force correct direction on first load
                setTimeout(() => {
                    $slider.trigger('refresh.owl.carousel');
                }, 10);
            }
        }
    });
}

 
/*
*
*
*/
function wpestate_property_slider_v3(slider_id, items) {
    // Initialize RTL value checking both data attribute and control_vars
    let rtl_value = false;
    const $slider = jQuery('#' + slider_id);
    
    // Check both data-rtl and control_vars.is_rtl
    if ($slider.attr('data-rtl') === 'yes' || control_vars.is_rtl === '1') {
        rtl_value = true;
        $slider.addClass('owl-rtl');
    }

    $slider.owlCarousel({
        loop: true,
        margin: 0,
        nav: true,
        items: 1,
        dots: true,
        mouseDrag: true,
        video: true,
        autoHeight: true,
        stagePadding: 0,
        URLhashListener: false,
        rtl: rtl_value,
        navText: [
            // Swap arrows for RTL
            rtl_value ? 
                '<i class="fas fa-arrow-right"></i>' : 
                '<i class="fas fa-arrow-left"></i>',
            rtl_value ? 
                '<i class="fas fa-arrow-left"></i>' : 
                '<i class="fas fa-arrow-right"></i>'
        ],
        onInitialized: function() {
            if (rtl_value) {
                const stage = $slider.find('.owl-stage');
                stage.css('transform-origin', 'center center');
                // Force correct direction on first load
                setTimeout(() => {
                    $slider.trigger('refresh.owl.carousel');
                }, 10);
            }
        }
    });
}


jQuery(window).on('hashchange', function(event) {
        event.preventDefault();
        history.pushState("", document.title, window.location.pathname + window.location.search); // Clear the hash
});


    
/**
* Initialize and configure the WpResidence property slider carousel
* 
* This function sets up the Owl Carousel for property images with navigation controls,
* RTL support, and indicator synchronization. It handles both horizontal and vertical
* carousel layouts, updates active indicators, and manages indicator sliding.
* 
* Dependencies:
* - jQuery
* - Owl Carousel 2
* - control_vars.is_rtl global variable
* 
* @since 4.9.0
* @requires jQuery
* @requires owl.carousel.js
* 
* Event handlers:
* - Click on indicator slides to specific image
* - Carousel translation updates active indicators
* - Manages indicator positioning for overflow
* 
* Related functions:
* - wpestate_show_capture_vertical2()
* - wpestate_show_capture2()
* 
* Global variables used:
* - control_vars.is_rtl
* 
* @return {void}
*/
function wpestate_property_slider() {
    let is_rtl = false;
    if (control_vars.is_rtl === '1') {
        is_rtl = true;
    }

    var owl_slider_property = jQuery("#property_slider_carousel").owlCarousel({
        loop: true,
        margin: 0,
        nav: true,
        items: 1,
        dots: false,
        mouseDrag: true,
        video: true,
        autoHeight: true,
        rtl: is_rtl,
        navText: [
            '<button class="carousel-control-prev wpresidence-carousel-control " type="button" data-slide="prev"><i class="demo-icon icon-left-open-big"></i><span class="visually-hidden">Previous</span></button>',
            '<button class="carousel-control-next wpresidence-carousel-control " type="button" data-slide="next"><i class="demo-icon icon-right-open-big"></i><span class="visually-hidden">Previous</span></button>'
        ],
        URLhashListener: false,
    });

    var hash;
    var number;

    jQuery('li[data-slide-to]').on('click', function(event) {
        event.preventDefault();
        var slideTo = parseInt(jQuery(this).data('slide-to'));
        owl_slider_property.trigger('to.owl.carousel', [slideTo, 300, true]);
    });

    owl_slider_property.on('translated.owl.carousel', function(event) {
        number = jQuery(".carousel-inner .owl-item.active .item").attr('data-number');
        
        // Remove active class from all indicators
        jQuery('.carousel-round-indicators a').removeClass('active');
        jQuery('.carousel-indicators a').removeClass('active');
        jQuery('.carousel-indicators-classic li').removeClass('active');
        jQuery('.carousel-indicators-vertical li').removeClass('active');
        jQuery('.carousel-indicators li').removeClass('active');

        // Add active class to current indicator
        const activeIndex = parseInt(number) - 1;
        
        if (jQuery(this).hasClass('carouselvertical')) {
            // Handle vertical indicators
            const $activeIndicator = jQuery('.carousel-indicators-vertical li[data-slide-to="' + activeIndex + '"]').addClass('active');
            const $container = jQuery('.carousel-indicators-vertical');
            
            if ($activeIndicator.length && $container.length) {
                const containerHeight = $container.height();
                const indicatorOffset = $activeIndicator.position()?.top || 0;
                const scrollAmount = Math.max(0, indicatorOffset - (containerHeight / 2) + 45);
                
                $container.css('transform', `translateY(-${scrollAmount}px)`);
            }
        } else {
            // Handle horizontal indicators
            let  $activeIndicator = jQuery('.carousel-indicators li[data-slide-to="' + activeIndex + '"]').addClass('active');
            let  $container = jQuery('.wpestate_property_carousel.carouselhorizontal .carousel-indicators');

            if(jQuery('.wpestate_classic_slider_wrapper').length>0){
                 $activeIndicator = jQuery('.carousel-indicators li[data-slide-to="' + activeIndex + '"]').addClass('active');
                 $container = jQuery('.wpestate_classic_slider_wrapper .carousel-indicators');
            }


            if ($activeIndicator.length && $container.length) {
                const containerWidth = $container.width();   
                const indicatorOffset = $activeIndicator.position()?.left || 0; 
                const scrollAmount = Math.max(0, indicatorOffset - 100); // 100px buffer from left 
                $container.css('transform', `translateX(-${scrollAmount}px)`);
            }
        }

        
    });
}




/*
*
*
*/
function wpestate_property_slider_2(){
    var autoplayvalue=false;
    var autoplayTimeoutvalue= parseInt( jQuery(".theme_slider_3").attr('data-auto'),10);

    if(autoplayTimeoutvalue>0){
        autoplayvalue=true;
    }
    let is_rtl=false;
    if(control_vars.is_rtl==='1'){
         is_rtl=true;
    }

   var owl = jQuery(".property_slider2_wrapper").owlCarousel({
        loop:true,
        margin:0,
        nav:true,
        items:1,
        dots:false,
        rtl:is_rtl,
        autoplay:autoplayvalue,
        autoplayTimeout:autoplayTimeoutvalue,
        navText : ["<div class='nextright'><i class='demo-icon icon-right-open-big'></i></div>","<div class='nextleft'><i class='demo-icon icon-left-open-big'></i></div>"],
        URLhashListener:true,

    });

    var hash;
    owl.on('translated.owl.carousel', function(event) {

        hash = jQuery(".property_slider2_wrapper .owl-item.active .item").attr('data-hash');

        jQuery('.property_slider2_wrapper-indicators a').removeClass('active');
        jQuery('a[href="#'+hash+'"]').addClass('active');
    });


}

/*
*
*
*/
function wpestate_theme_slider_3(){
    var autoplayvalue=false;
    var autoplayTimeoutvalue= parseInt( jQuery(".theme_slider_3").attr('data-auto'),10);

    if(autoplayTimeoutvalue>0){
        autoplayvalue=true;
    }
    let is_rtl=false;
    if(control_vars.is_rtl==='1'){
         is_rtl=true;
    }

   var owl = jQuery(".theme_slider_3").owlCarousel({
        loop:true,
        margin:0,
        nav:true,
        items:1,
        dots:false,
        rtl:is_rtl,
        autoplay:autoplayvalue,
        autoplayTimeout:autoplayTimeoutvalue,
        navText : [
            '<svg xmlns="http://www.w3.org/2000/svg" width="32.414" height="20.828" viewBox="0 0 32.414 20.828"><g id="Group_30" data-name="Group 30" transform="translate(-1845.086 -1586.086)"><line id="Line_2" data-name="Line 2" x1="30" transform="translate(1846.5 1596.5)" fill="none" stroke="#fff" stroke-linecap="round" stroke-width="2"></line><line id="Line_3" data-name="Line 3" x1="9" y2="9" transform="translate(1846.5 1587.5)" fill="none" stroke="#fff" stroke-linecap="round" stroke-width="2"></line><line id="Line_4" data-name="Line 4" x1="9" y1="9" transform="translate(1846.5 1596.5)" fill="none" stroke="#fff" stroke-linecap="round" stroke-width="2"></line></g></svg>',
            '<svg xmlns="http://www.w3.org/2000/svg" width="32.414" height="20.828" viewBox="0 0 32.414 20.828"><g id="Symbol_1_1" data-name="Symbol 1 – 1" transform="translate(-1847.5 -1589.086)"><line id="Line_5" data-name="Line 2" x2="30" transform="translate(1848.5 1599.5)" fill="none" stroke="#fff" stroke-linecap="round" stroke-width="2"></line><line id="Line_6" data-name="Line 3" x2="9" y2="9" transform="translate(1869.5 1590.5)" fill="none" stroke="#fff" stroke-linecap="round" stroke-width="2"></line><line id="Line_7" data-name="Line 4" y1="9" x2="9" transform="translate(1869.5 1599.5)" fill="none" stroke="#fff" stroke-linecap="round" stroke-width="2"></line></g></svg>',
            ],
        URLhashListener:true,

    });

    var hash;
    owl.on('translated.owl.carousel', function(event) {

        hash = jQuery(".theme_slider_3 .owl-item.active .item").attr('data-hash');

        jQuery('.theme_slider_3_carousel-indicators a').removeClass('active');
        jQuery('a[href="#'+hash+'"]').addClass('active');
    });


}


/*
*
*
*/
function estate_sidebar_slider_carousel(){
    "use strict";
    let is_rtl=false;
    if(control_vars.is_rtl==='1'){
         is_rtl=true;
    }

    // Check if owlCarousel is available
    if (typeof jQuery.fn.owlCarousel === 'function') {
        jQuery(".owl-featured-slider").owlCarousel({
            loop:true,
            margin:0,
            nav:true,
            items:1,
            dots:false,
            rtl:is_rtl,
            navText : [
                '<button class="carousel-control-prev wpresidence-carousel-control " type="button" data-slide="prev"><i class="demo-icon icon-left-open-big"></i><span class="visually-hidden">Previous</span></button>',
                '<button class="carousel-control-next wpresidence-carousel-control " type="button" data-slide="next"><i class="demo-icon icon-right-open-big"></i><span class="visually-hidden">Previous</span></button>'
            ],
        });
    }else {
        console.warn("owlCarousel function not found.");
    }

}
 


/*
*  
*
*/
function estate_start_lightbox_modal(){
    "use strict";
    var jump_slide;

    var owl = jQuery("#owl-demo-modal");
    owl.trigger("destroy.owl.carousel");

  owl.owlCarousel({
        loop:true,
        margin:0,
        nav:true,
        items:1,
        dots:false, 
        startPosition:1,
        navText : [
            '<button class="carousel-control-prev wpresidence-carousel-control " type="button" data-slide="prev"><i class="demo-icon icon-left-open-big"></i><span class="visually-hidden">Previous</span></button>',
            '<button class="carousel-control-next wpresidence-carousel-control " type="button" data-slide="next"><i class="demo-icon icon-right-open-big"></i><span class="visually-hidden">Previous</span></button>'
        ],
    });


    jQuery('.lightbox_trigger').on( 'click', function(event) {



        event.preventDefault();
        jump_slide=parseInt( jQuery(this).attr('data-slider-no') );
        var carousel = jQuery("#owl-demo-modal");
        carousel.owlCarousel();
          carousel.trigger("refresh.owl.carousel");

        carousel.trigger("to.owl.carousel", [jump_slide, 1, true]);
        jQuery('.lightbox_property_wrapper').show();

    });

    jQuery('.lighbox-image-close').on( 'click', function(event) {
        event.preventDefault();
        jQuery('.lightbox_property_wrapper').hide();
    });
}




/**
 * WPResidence Blog List Widget AJAX Handler
 *
 * Handles AJAX loading of additional blog posts for the blog list widget.
 *
 * @package WPResidence
 * @subpackage Widgets
 * @since 1.0.0
 */
function wpestate_blog_list_widget() {
    "use strict";

    var ajaxurl = ajaxcalls_vars.wpestate_ajax;

    jQuery('.blog_list_loader').on('click', function (event) {
        var $button = jQuery(this);
        var $container = $button.parent().parent();
        var $loader = $container.find('.wpestate_listing_sh_loader');
        var $grid = $container.find('.items_shortcode_wrapper_grid');

        var page = parseInt($container.attr('data-page'), 10) + 1;
        $container.attr('data-page', page);



        //$loader.show(); 
        if ($container.find('.items_shortcode_wrapper_grid').length>0 ){
            wpestate_createSkeletons($container.find('.items_shortcode_wrapper_grid'),true);
        }else{
            wpestate_createSkeletons($container.find('.blog25_shortcode_wrapper'),true);
        }

        jQuery.ajax({
            type: 'POST',
            dataType: 'json',
            url: ajaxurl,
            data: {
                action: 'wpestate_load_blog_list_widget_wrapper',
                control_terms_id: $container.attr('data-category_ids'),
                number: $container.attr('data-number'),
                page: page,
                rownumber: $container.attr('data-row-number'),
                card_version: $container.attr('data-card-version'),
                sort_by: $container.attr('data-sort-by'),
                display_grid: $container.attr('data-display-grid')
            },
            success: function (data) {
          
               
                if (data.success) {
                    if ($grid.length) {
                        if (data.results === 0) {
                         //   $loader.before(data.html).hide();
                            $button.hide();
                        } else {
                            //$grid.append(data.html);
                            wpestate_replaceSkeletons($container.find('.items_shortcode_wrapper_grid'), data.html,true)
                        }
                    } else {
                       // $loader.before(data.html).hide();
                        wpestate_replaceSkeletons($container.find('.blog25_shortcode_wrapper'), data.html,true)
                        if (data.results === 0) {
                            $button.hide();
                        }
                    }
                } else {
                    $button.hide();
                }
                $loader.hide();
            },
            error: function (xhr, status, error) {
                console.error('AJAX request failed:', error);
                $loader.hide();
            }
        });
    });
}



/*
*
*
*/
function wpestate_property_list_sh(ajax_loader,ajax_filters){
    "use strict";
    var    ajaxurl         =   ajaxcalls_vars.wpestate_ajax;
    jQuery(ajax_loader).click( function(event) {
        console.log('wpestate_property_list_sh');

        var container               =   jQuery(this).parent().parent();
        var type                    =   container.attr('data-type');
        var category_ids            =   container.attr('data-category_ids');
        var action_ids              =   container.attr('data-action_ids');
        var city_ids                =   container.attr('data-city_ids');
        var area_ids                =   container.attr('data-area_ids');
        var state_ids               =   container.attr('data-state_ids');
        var status                  =   container.attr('data-status_ids');
        var features                =   container.attr('data-features_ids');
        var number                  =   container.attr('data-number');
        var align                   =   container.attr('data-align');
        var show_featured_only      =   container.attr('data-show_featured_only');
        var random_pick             =   container.attr('data-random_pick');
        var featured_first          =   container.attr('data-featured_first');
        var page                    =   container.attr('data-page');
        var align                   =   container.attr('data-align');
        var row_number              =   container.attr('data-row-number');
        var card_version            =   container.attr('data-card-version');
        var sort_by                 =   container.attr('data-sort-by');
        var display_grid            =   container.attr('data-display-grid');
        var agentid                =   container.attr('data-agent-id');

      
        page=parseInt(page);
        page=page+1;
        container.attr('data-page',page);
        //container.find('.wpestate_listing_sh_loader').show();

       
   
        // Show skeleton placeholders while loading
        //wpestate_createSkeletons(container.find('.items_shortcode_wrapper'),true);

        if (container.find('.items_shortcode_wrapper_grid').length>0 ){
            wpestate_createSkeletons(container.find('.items_shortcode_wrapper_grid'),true);
        }else{
            wpestate_createSkeletons(container.find('.items_shortcode_wrapper'),true);
        }

        page=parseInt(page);

        // remove no results message if the we change the tab
        if(page===1){
          container.find('.no_results').remove();
        }



        var nonce = jQuery('#wpestate_ajax_filtering').val();
        jQuery.ajax({
        type: 'POST',
        dataType: 'json',
        url: ajaxurl,
        data: {
            'action'                :   'wpestate_load_recent_items_sh',
            'type'                  :   type,
            'category_ids'          :   category_ids,
            'action_ids'            :   action_ids,
            'city_ids'              :   city_ids,
            'area_ids'              :   area_ids,
            'state_ids'             :   state_ids,
            'status'                :   status,
            'features'              :   features,
            'number'                :   number,
            'align'                 :   align,
            'show_featured_only'    :   show_featured_only,
            'random_pick'           :   random_pick,
            'featured_first'        :   featured_first,
            'page'                  :   page,
            'row_number'            :   row_number,
            'card_version'          :   card_version,
            'sort_by'              :   sort_by,
            'display_grid'          :   display_grid,
            'agentid'              :   agentid,
            'security'              :   nonce
        },


        success: function (data) {
       
            if (data.success == true && data.html !== '') {

                //container.find('.wpestate_listing_sh_loader').hide();

                // Replace skeleton placeholders with real cards sequentially
                if (data.no_results === true) {
                    // Remove skeletons and show no results message
                    container.find('.skeleton-placeholder').remove();
                    container.find('.wpresidence_button').after(data.html).hide();
                } else {
                    // Replace skeletons one by one with smooth animation
            
                 
                    if (container.find('.items_shortcode_wrapper_grid').length>0 ){
                       
                        wpestate_replaceSkeletons(container.find('.items_shortcode_wrapper_grid'), data.html,true);
                    }else{
                        wpestate_replaceSkeletons(container.find('.items_shortcode_wrapper'), data.html,true);
                    }
                }

                // Restart JavaScript functionality after AJAX load
               // wpestate_restart_js_after_ajax();
                
                // Handle custom image wrapping
                jQuery('.col-md-12.listing_wrapper .property_unit_custom_element.image').each(function(){
                    jQuery(this).parent().addClass('wrap_custom_image');
                });
                var wrap_image = parseInt( jQuery('.wrap_custom_image').width());

                if(wrap_image!=0){
                    jQuery('.col-md-12>.property_listing_custom_design').css('padding-left',wrap_image);
                }

            } else {
                // Remove skeletons and hide load more button on failure
                container.find('.skeleton-placeholder').remove();
                jQuery(ajax_loader).hide();
            }
        },
        error: function (errorThrown) {
            // Remove skeletons on error too
            container.find('.skeleton-placeholder').remove();
        }
    });//end ajax


    });


    jQuery(ajax_filters).on( 'click', function(event) {

        var container   =   jQuery(this).parent().parent();
        var taxid       =   jQuery(this).attr('data-taxid');
        var taxonomy    =   jQuery(this).attr('data-taxonomy');

        jQuery(this).parent().parent().find('.wpestate_item_list_sh').show();

        switch(taxonomy) {
            case 'property_category':

                var category_ids            =   container.attr('data-category_ids');
                category_ids = wpestate_replace_tax_id(jQuery(this),category_ids,taxid);
                container.attr('data-category_ids',category_ids);

                break;
            case 'property_action_category':

                var action_ids            =   container.attr('data-action_ids');
                action_ids = wpestate_replace_tax_id(jQuery(this),action_ids,taxid);
                container.attr('data-action_ids',action_ids);

                break;
            case 'property_city':

                var city_ids            =   container.attr('data-city_ids');
                city_ids = wpestate_replace_tax_id(jQuery(this),city_ids,taxid);
                container.attr('data-city_ids',city_ids);

                break;
            case 'property_area':

                var area_ids            =   container.attr('data-area_ids');
                area_ids = wpestate_replace_tax_id(jQuery(this),area_ids,taxid);
                container.attr('data-area_ids',area_ids);

                break;
            case 'property_county_state':


                var state_ids            =   container.attr('data-state_ids');
                state_ids = wpestate_replace_tax_id(jQuery(this),state_ids,taxid);
                container.attr('data-state_ids',state_ids);

                break;

            case 'property_status':
                var status_ids            =   container.attr('data-status_ids');
                status_ids = wpestate_replace_tax_id(jQuery(this),status_ids,taxid);

                container.attr('data-status_ids',status_ids);

                break;
            case 'property_features':
                var status_ids            =   container.attr('data-features_ids');
                status_ids = wpestate_replace_tax_id(jQuery(this),status_ids,taxid);

                container.attr('data-features_ids',status_ids);

                break;
    

        }
        container.attr('data-page',0);
        jQuery(this).toggleClass('tax_active');
        
        //container.find('.items_shortcode_wrapper_grid').empty();
        //container.find('.listing_wrapper').remove();

        // Poll to check if skeletons exist, then remove original cards
        var pollCount = 0;
        var pollInterval = setInterval(function() {
            if (container.find('.skeleton-placeholder').length > 0 || pollCount > 10) {
                clearInterval(pollInterval);
                // Now safe to remove original cards
               // container.find('.items_shortcode_wrapper_grid').empty();
            
                 
                    container.find('.shortcode_wrapper_grid_item').has('.listing_wrapper:not(.skeleton-placeholder)').remove();    
                    container.find('.listing_wrapper:not(.skeleton-placeholder)').remove();



                    container.find('.shortcode_wrapper_grid_item').each(function(){
                    var item = jQuery(this);
                  
                    
                    if(item.children('.listing_wrapper').length > 0){
                       // console.log('keep it');
                    } else {
                        //console.log('should not keep');
                    }
                    });
                container.find('.shortcode_wrapper_grid_item:empty').remove();
                container.find('.shortcode_wrapper_grid_item').each(function(){
                    if(jQuery(this).is(':empty')){
                        jQuery(this).remove();
                    }
                });
            }
            pollCount++;
        }, 150); // Check every 50ms





        container.find('.wpestate_item_list_sh').trigger('click');

    });


    function wpestate_replace_tax_id(acesta,tax_ids,taxid){


        if(!acesta.hasClass('tax_active')){
            if ( tax_ids.indexOf(taxid) >= 0){
                return taxid+",";
            }else{
                tax_ids=tax_ids+taxid+",";
            }
        }else{
            var to_replace=taxid+",";
            tax_ids=tax_ids.replace(to_replace , "");
        }
        return tax_ids;

    }

}


/**
 * WpEstate Open Menu Function
 * 
 * This function sets up event listeners for the header phone and user menu elements.
 * It handles the toggling of the user menu and shopping cart visibility.
 *
 * @since 1.0.0
 * 
 * @requires jQuery
 * 
 * @return {void}
 * 
 * @example
 * // Call this function after the DOM is ready
 * jQuery(document).ready(function($) {
 *     wpestate_open_menu();
 * });
 */
function wpestate_open_menu() {
    "use strict";

    // Prevent click event propagation on header phone
    jQuery('.header_phone').on('click', function(event) {
        event.stopPropagation();
    });

    // Handle click event on logged-in user menu
    jQuery('#user_menu_u.user_loged').on('click', function(event) {
        // Hide shopping cart
        jQuery('#wpestate_header_shoping_cart').fadeOut(400);

        // Toggle user menu visibility
        if (jQuery('#user_menu_open').is(":visible")) {
            // If menu is visible, hide it and remove 'opensvg' class
            jQuery('#user_menu_open').removeClass('iosfixed').fadeOut(400);
            jQuery('.navicon-button').removeClass('opensvg');
        } else {
            // If menu is hidden, show it and add 'opensvg' class
            jQuery('#user_menu_open').fadeIn(400);
            jQuery('.navicon-button').addClass('opensvg');
        }

        // Prevent event from bubbling up to parent elements
        event.stopPropagation();
    });
}

/**
 * Check if the current device is mobile or tablet
 * 
 * This function uses Bootstrap 5.3's breakpoint checking to determine
 * if the current device is considered mobile or tablet.
 * 
 * @returns {boolean} True if the device is mobile or tablet, false otherwise
 */
function wpestate_isMobileOrTablet() {
    // Check if the window width is less than the 'xl' breakpoint
    return window.innerWidth < 1200; // Bootstrap's 'xl' breakpoint is 1200px
}


/*
*
*
*/
function wpestate_contact_us_shortcode(){
    "use strict";
    jQuery('#btn-cont-submit_sh').on( 'click', function(event) {
        var parent,contact_name, contact_email, contact_phone, contact_coment, agent_email, property_id, nonce, ajaxurl;
        contact_name    =   jQuery('#foot_contact_name_sh').val();
        contact_email   =   jQuery('#foot_contact_email_sh').val();
        contact_phone   =   jQuery('#foot_contact_phone_sh').val();
        contact_coment  =   jQuery('#foot_contact_content_sh').val();
        nonce           =   jQuery('#agent_property_ajax_nonce').val();
        ajaxurl         =   ajaxcalls_vars.admin_url + 'admin-ajax.php';
        parent          =   jQuery(this).parent().parent();
   
        if(ajaxcalls_vars.use_gdpr==='yes'){
            if (!parent.find('.wpestate_agree_gdpr').is(':checked')) {
                jQuery('#footer_alert-agent-contact_sh').empty().removeClass('wpestate-agent-contact-sent').append(ajaxcalls_vars.gdpr_terms);
                return;
            }
        }
     
        jQuery('#footer_alert-agent-contact_sh').empty().append(ajaxcalls_vars.sending);

        jQuery.ajax({
            type: 'POST',
            dataType: 'json',
            url: ajaxurl,
            data: {
                'action'    :   'wpestate_ajax_agent_contact_form',
                'name'      :   contact_name,
                'email'     :   contact_email,
                'phone'     :   contact_phone,
                'comment'   :   contact_coment,
                'nonce'     :   nonce
            },
            success: function (data) {

                if (data.sent) {
                    jQuery('#foot_contact_name_sh').val('');
                    jQuery('#foot_contact_email_sh').val('');
                    jQuery('#foot_contact_phone_sh').val('');
                    jQuery('#foot_contact_content_sh').val('');
                    jQuery('#footer_alert-agent-contact_sh').empty().addClass('wpestate-agent-contact-sent').append(data.response);
                }else{
                    jQuery('#footer_alert-agent-contact_sh').empty().removeClass('wpestate-agent-contact-sent').append(data.response);
                }

            },
            error: function (errorThrown) {

            }
        });
    });

}







/*
*
*
*/
function wpestate_enable_property_modal(listing_id,main_img_url,main_title,link){
    "use strict";

    if (Modernizr.mq('only all and (max-width: 1024px)')) {
        return;
    }



    jQuery('.website-wrapper').css('position','fixed');
    jQuery('#property_modal_images').empty();
    var ajaxurl     =   ajaxcalls_vars.wpestate_ajax;
    var window_height =jQuery( window ).height();

    jQuery("#property_modal_images") .css('height', window_height+'px');
    window_height=window_height-183;
    jQuery("#property_modal_content").css('height', window_height+'px');

    jQuery('#modal_property_agent').empty();
    jQuery('.modal_content_block').empty();
    jQuery('#property_modal_content .modal_property_description').empty();
    jQuery('#property_modal_content .modal_property_adress').empty();
    jQuery('#property_modal_content .modal_property_details').empty();
    jQuery('#property_modal_content .modal_property_features').empty();
    jQuery('#property_modal_content .modal_property_video').empty();
    jQuery('#property_modal_content .modal_property_video_tour').empty();
    jQuery('#property_modal_content .modal_property_walkscore').empty();
    jQuery('#property_modal_content .modal_property_floor_plans').empty();
    jQuery('#modal_property_mortgage').empty();
    jQuery('#modal_property_maps').empty();
    jQuery('#property_details_modal_wrapper').show();

    //prepopoluate
    var to_apped    ='<img src="'+main_img_url+'" data-slider-no="1" class="img-responsive lightbox_trigger" alt="image">';

    jQuery('#property_modal_images').append(to_apped);
    jQuery('#property_modal_header .modal_property_title').html(main_title);
    jQuery('#owl-demo-modal').empty();


    wpestate_propery_preview_prepopulate();


    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        dataType: 'json',
        data: {
            'action'            :   'wpestate_property_modal_listing_details',
            'postid'            :   listing_id,

         //   'security'          :   nonce,
        },
        success: function (data) {

            if(data.response===true){


                data.images.forEach(wpestate_populate_modal_images);
                jQuery('.property_placeholder').remove();
                data.full_images.forEach(wpestate_populate_modal_images_full);
                wpestate_populate_content(data);

            }
        },
        error: function (errorThrown) {

        }
    });//end ajax

    ///
    /// aysn request no 2
    ////////////////////////////////////////////////////////////////////////////
     jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        dataType: 'json',
        data: {
            'action'            :   'wpestate_property_modal_listing_details_second',
            'postid'            :   listing_id,

         //   'security'          :   nonce,
        },
        success: function (data) {

            if(data.response===true){

                jQuery('#property_modal_content .modal_property_yelp').empty().append(data.yelp);
               // jQuery('#property_modal_content .modal_property_walkscore').empty().append(data.walkscore);
                jQuery('#property_modal_content .modal_property_floor_plans').empty().append(data.floor_plans);
                jQuery('#modal_property_mortgage').empty().append(data.mortgage);
                jQuery('#modal_property_maps').empty().append(data.map);


                //floor plans
                wpestate_enable_floor_plans();
                estate_start_lightbox_floorplans();

                //mortgage
                wpestate_show_morg_pie();

                // map
      
                wpestate_map_shortcode_function();

            }
        },
        error: function (errorThrown) {

        }
    });//end ajax
}


/*
*
*
*/

function wpestate_populate_content(data){
    jQuery('#property_modal_top_bar').empty();
    jQuery('#property_modal_top_bar').append(data.favorite);
    jQuery('#property_modal_top_bar').append(data.share);


    jQuery('#property_modal_header .modal_property_title').html('<a href="'+data.link+'" target="_blank">'+data.title+'</a>');
    jQuery('#property_modal_header .modal_property_price').html(data.price);
    jQuery('#property_modal_header .modal_property_bed').html(data.beds_section);
    jQuery('#property_modal_header .modal_property_addr').html(data.addr_section);

    
    jQuery('#modal_property_agent').empty().append(data.agent_section);
    jQuery('#property_modal_content .modal_property_description').empty().append(data.content);
    jQuery('#property_modal_content .modal_property_adress').empty().append(data.address);
    jQuery('#property_modal_content .modal_property_details').empty().append(data.details);
    jQuery('#property_modal_content .modal_property_features').empty().append(data.features);
    jQuery('#property_modal_content .modal_property_video').empty().append(data.video);
    jQuery('#property_modal_content .modal_property_video_tour').empty().append(data.video_tour);

    //contact
     wpestate_agent_contact_modals();
    //img
    estate_start_lightbox_modal();

    // video
    if (jQuery(".venobox").length > 0){
        jQuery('.venobox').venobox();
    }

    //favorite
    wpestate_add_to_favorites();

    //print
     wpestate_print_property_page();

}

/*
*
*
*/
function wpestate_populate_modal_images(value,index){

    if(  jQuery('.property_placeholder').length>0 ){
        var placeholder= jQuery('.property_placeholder:first');
        placeholder.attr('src',value).removeClass('property_placeholder');
    }else{
        var to_apped='<img src="'+value+'" data-slider-no="'+index+'" class="img-responsive lightbox_trigger" alt="image">';

        jQuery('#property_modal_images').append(to_apped);
    }


}

/*
*
*
*/
function wpestate_populate_modal_images_full(value,index){
   // var to_append2='<div class="item"><img src="'+value+'"></div>';
     var  to_append2='<div class="owl_holder" style="background-image:url('+value+')"></div>';
    jQuery('#owl-demo-modal').append(to_append2);
}


/*
*
*
*/
function wpestate_propery_preview_prepopulate(){

    path_image=control_vars.path+'/img/placeholder1.png';
    for (i = 0; i <14; i++) {
        var to_apped='<img src="'+path_image+'" class="img-responsive  lightbox_trigger property_placeholder" data-slider-no="'+(i+1)+'" alt="image">';
        jQuery('#property_modal_images').append(to_apped);
    }
}

/*
*
*
*/

function wpestate_agent_contact_modals(){
    wpestate_agent_submit_email();

    wpestate_enable_schedule_contact();

    jQuery( "#modal_contact_agent").unbind( "click" );
    jQuery('#modal_contact_agent').on('click',function(event){
        jQuery('.schedule_wrapper').hide();
        jQuery('#property_modal_content .agent_contanct_form').slideDown();

        jQuery('#property_modal_content').animate({
                scrollTop: 0
            },
            "slow"
        );
    });



}

/*
*
*
*/

function wpestate_get_location(){

    if(control_vars.location_animation==='no'){
      return;
    }
    if(  jQuery('.heading_over_image').length===0){
        return;
    }

    jQuery.get("https://ipapi.co/json", function(place) {
        var city = place.city;

        if(city == null ||  parseInt( jQuery('.home').length)===0 ){
             //do nothing
            }else{
                var heading = control_vars.location_animation_text;

                heading = heading.replace("%city%", city);

                jQuery('.home .heading_over_image,.home .heading_over_video').slideUp("300",function(){
                    jQuery('.home .heading_over_image,.home .heading_over_video').text(heading);
                    jQuery('.home .heading_over_image,.home .heading_over_video').slideDown();
                });

            }
        }, "json");
}

/**
 * WpEstate Sliding Box Interaction
 *
 * This function sets up the interactive behavior for the sliding box elements
 * in the WpResidence theme. It handles the mouseover event to activate and
 * deactivate sliding boxes.
 *
 * @since 1.0.0
 * 
 * @requires jQuery
 */
function wpestate_slider_box() {
    
    // Attach a mouseover event handler to all elements with class 'wpestate_sliding_box'
    jQuery(".wpestate_sliding_box").mouseover(function() {
        // 'this' refers to the current element being hovered over
        var selected_item = jQuery(this);
        
        // Find the parent container of the current sliding box
        var item_parent = selected_item.parent();
        
        // Remove 'active-element' class from all sliding boxes within the same parent
        item_parent.find('.wpestate_sliding_box').removeClass('active-element');
        
        // Add 'active-element' class to the currently hovered sliding box
        selected_item.addClass('active-element');
    });
}



/*
*Mobile Menu Slide out
*
*/
function wpestate_mobile_menu_slideout(){
    var vc_size;
    var var_parents=new Array();
    var var_parents_back=new Array();

  


    if(jQuery('.mobile-trigger-user').length>0){
        var slideout_user_menu = new Slideout({
            'panel': document.getElementById('all_wrapper'),
            'menu': document.getElementById('mobilewrapperuser'),
            'padding': -256,
            'tolerance': 70
        });

        slideout_user_menu.disableTouch();

        // Toggle button
        document.querySelector('.mobile-trigger-user').addEventListener('click', function() {
            slideout_user_menu.toggle();
           // jQuery('#mobilewrapperuser').show();
            if(jQuery('#mobilewrapperuser').is(':visible') ){
                jQuery('#mobilewrapperuser').hide();
            }else{
                jQuery('#mobilewrapperuser').show();
            }
        
        });
       
        jQuery('.mobilemenu-close-user').on( 'click', function(event) {
            slideout_user_menu.toggle();
            setTimeout(function() { 
                jQuery('#mobilewrapperuser').hide(); 
            }, 200);
    
        });

    }



    
    if(jQuery('.mobile-trigger').length>0){

        var slideout_link_menu = new Slideout({
            'panel': document.getElementById('all_wrapper'),
            'menu': document.getElementById('mobilewrapper_links'),
            'padding': 256,
            'tolerance': 70,
            'side': 'left',
            'easing': 'cubic-bezier(.32,2,.55,.27)'
        });
        slideout_link_menu.disableTouch();
        document.querySelector('.mobile-trigger').addEventListener('click', function() {
            slideout_link_menu.toggle();
          
            if(jQuery('#mobilewrapper_links').is(':visible') ){
                jQuery('#mobilewrapper_links').hide();
            }else{
                jQuery('#mobilewrapper_links').show();
            }
        });

        jQuery('.mobilemenu-close').on( 'click', function(event) {
            slideout_link_menu.toggle();
            setTimeout(function() { 
                jQuery('#mobilewrapper_links').hide();
            }, 200);
        });
    }
    
}

/*
* Mobile Menu open submenu
*/

function wpestate_mobile_menu_open_submenu(){
    jQuery('.mobilex-menu li').on( 'click', function(event) {
        event.stopPropagation();
        var selected;
        selected = jQuery(this).find('.sub-menu:first');
        selected.slideToggle();
    });

}



/*
* Mobile Menu Elementor open/close 
*/

function wpestate_elementor_mobile_menu_trigger_open(){
    jQuery('.wpestate_mobile_menu_trigger').on('click', function(event) {
            jQuery(this).hide();
            const parent = jQuery(this).parent();
            const openButton=parent.find('.wpestate_mobile_menu_trigger_close');
            var menu = parent.find('.mobilex-menu');


            jQuery('.wpestate_mobile_menu_trigger_close').show();
            // Show the menu first to measure it correctly
            menu.css('display', 'block');

            var containerRect = openButton[0].getBoundingClientRect();

            var margin = 0; // Margin from the viewport edges

            // Calculate the left position of the child
            var childLeft = -containerRect.left +margin;

            // Apply the calculated positions
           // menu.css('left', childLeft + 'px');
            menu.css('width', 'calc(100vw - ' + (margin * 2) + 'px)');

   
    });

    jQuery('.wpestate_mobile_menu_trigger_close').on('click',function(event){
        jQuery(this).hide();
        const parent=jQuery(this).parent();    
        var menu = parent.find('.mobilex-menu');
        menu.css('transform', '');
        jQuery('.wpestate_mobile_menu_trigger').show();
        parent.find('.mobilex-menu').hide();
    });

}


/*
* Control media buttons
*/

function wpestate_control_media_buttons(){
    var curentID;

    jQuery('.wpestate_control_media_button').on('click',function(){
        jQuery('.wpestate_control_media_button').removeClass('slideron');
        jQuery(this).addClass('slideron');
        jQuery('.wpestate_property_media_section_wrapper .status-wrapper').hide();

        jQuery('.wpestate_property_slider_thing').hide();
        var div_to_show=jQuery(this).attr('data-show');

        jQuery('.'+div_to_show).show();
        if(div_to_show=='wpestate_property_carousel'){
            jQuery('.wpestate_property_media_section_wrapper .status-wrapper').show();
        }

        if(div_to_show=='google_map_slider_wrapper'){
            wpestate_control_media_emable_map();
        } 
        
        if(jQuery(this).attr('id')=='slider_enable_street'){
            wpestate_control_media_emable_street_view();
        }

        
    });
}



/*
*
* Enable Street Map
*
*/


function wpestate_control_media_emable_street_view(){
    var cur_lat, cur_long, myLatLng;

    cur_lat     =   jQuery('#googleMapSlider').attr('data-cur_lat');
    cur_long    =   jQuery('#googleMapSlider').attr('data-cur_long');
    myLatLng    =   new google.maps.LatLng(cur_lat,cur_long);

    jQuery('#gmapzoomplus.smallslidecontrol,#gmapzoomminus.smallslidecontrol,.google_map_poi_marker ').hide();

    panorama.setPosition(myLatLng);
    panorama.setVisible(true);
}

/*
*
* Enable Map for Slider
*
*/

function wpestate_control_media_emable_map(){
    jQuery('#googleMapSlider').show();
    jQuery('#gmapzoomplus.smallslidecontrol,#gmapzoomminus.smallslidecontrol,.google_map_poi_marker ').show();

    if(wp_estate_kind_of_map===1){
        google.maps.event.trigger(map, "resize");
    }else{
        setTimeout(function(){ map.invalidateSize(); }, 600);
    }

    cur_lat     =   jQuery('#googleMapSlider').attr('data-cur_lat');
    cur_long    =   jQuery('#googleMapSlider').attr('data-cur_long');

    if(wp_estate_kind_of_map===1){
        map.setOptions({draggable: true});
        myLatLng    =   new google.maps.LatLng(cur_lat,cur_long);
        map.setCenter(myLatLng);
        map.panBy(10,-100);
        panorama.setVisible(false);
    }
   
}
/*
*
* Get Cookie
*
*/
function wpestate_getCookie(cname) {
    var name = cname + "=";
    var ca = document.cookie.split(';');
    for(var i=0; i<ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0)==' ') c = c.substring(1);
        if (c.indexOf(name) == 0) return c.substring(name.length,c.length);
    }
    return "";
 }


 /*
*
* eanble advanced tab functions
*
*/

function wpestate_advtabs_function(item){

    var all_sliders = ['mainform','sidebar','shortcode','mobile','half'];
    var min_tab_slider=    control_vars.adv6_min_price[slider_counter];
    var max_tab_slider=    control_vars.adv6_max_price[slider_counter];

    slider_counter++;
    for (var i = 0, length = all_sliders.length; i < length; i++) {
        wpestate_enable_slider_tab_for_all(all_sliders[i],item,min_tab_slider,max_tab_slider);
    }
}


/*
*
* eanble advanced tab functions
*
*/


function wpestate_enable_slider_tab_for_all(position,item,min_tab_slider,max_tab_slider){

    var slider_id = 'slider_price_'+item+'_'+position ;
    var price_min = 'price_low_'+item;
    var price_max = 'price_max_'+item;
    var ammount   = 'amount_'+item+'_'+position;


    var my_custom_curr_symbol  =   decodeURI ( wpestate_getCookie('my_custom_curr_symbol') );
    var my_custom_curr_coef    =   parseFloat( wpestate_getCookie('my_custom_curr_coef'));
    var my_custom_curr_pos     =   parseFloat( wpestate_getCookie('my_custom_curr_pos'));
    var my_custom_curr_cur_post=   wpestate_getCookie('my_custom_curr_cur_post');

//    wpestate_enable_slider(slider_id, price_min, price_max, ammount, my_custom_curr_pos, my_custom_curr_symbol, my_custom_curr_cur_post, my_custom_curr_coef) ;
    wpestate_enable_slider_tab(min_tab_slider,max_tab_slider,'slider_price_'+item+'_'+position, 'price_low_'+item, 'price_max_'+item, 'amount_'+item+'_'+position, my_custom_curr_pos, my_custom_curr_symbol, my_custom_curr_cur_post,my_custom_curr_coef,control_vars.adv6_min_price[slider_counter],control_vars.adv6_max_price[slider_counter]);

    jQuery( '#slider_price_'+item +'_'+position).slider({
        stop: function( event, ui ) {
            if (typeof (wpestate_show_pins) !== "undefined") {
                first_time_wpestate_show_inpage_ajax_half=1;

                wpestate_show_pins();
            }
        }
    });
}


/*
*
* eanble slider tab
*/
function wpestate_enable_slider_tab(slider_min,slider_max,slider_name, price_low, price_max, amount, my_custom_curr_pos, my_custom_curr_symbol, my_custom_curr_cur_post, my_custom_curr_coef) {
 
    "use strict";
    var price_low_val, price_max_val, temp_min, temp_max;
    price_low_val = parseInt(jQuery('#'+price_low).val(), 10);
    price_max_val = parseInt(jQuery('#'+price_max).val(), 10);


    slider_min = parseInt(slider_min,10);
    slider_max = parseInt(slider_max,10);

    if (!isNaN(my_custom_curr_pos) && my_custom_curr_pos !== -1) {
        slider_min =slider_min *my_custom_curr_coef;
        slider_max =slider_max *my_custom_curr_coef;
    }

    var slider_parent =  jQuery("#" + slider_name).parent();

    var parent =  jQuery("#" + slider_name).parent().parent();
    var component_price_min;
    var component_price_max;
    var dropdown_parent;
    var dropdown_label;
    var component_label_value;

    if(parent.hasClass('wpestate-price-popoup-wrapper')){
        component_price_min=parent.find('#component_'+price_low);
        component_price_max=parent.find('#component_'+price_max);
        var dropdown_parent=parent.parent().find('.dropdown-toggle');
        dropdown_label = parent.find('.price_label_component');
    
    }



    jQuery("#" + slider_name).slider({
        range: true,
        min: parseFloat(slider_min),
        max: parseFloat(slider_max),
        values: [price_low_val, price_max_val ],
        slide: function (event, ui) {
            temp_min= ui.values[0].format() ;
            temp_max= ui.values[1].format();
            if(control_vars.indian_format==="yes"){
              temp_min=wpstrea_js_indian_format(ui.values[0]);
              temp_max=wpstrea_js_indian_format(ui.values[1]);
            }



            if (!isNaN(my_custom_curr_pos) && my_custom_curr_pos !== -1) {
                slider_parent.find("#" + price_low).val(ui.values[0]);
                slider_parent.find("#" + price_max).val(ui.values[1]);

                jQuery("#price_low").val(ui.values[0]);
                jQuery("#price_max").val(ui.values[1]);




                if (my_custom_curr_cur_post === 'before') {
                    jQuery("#" + amount).text( wpestate_replace_plus( decodeURIComponent ( my_custom_curr_symbol ) ) + " " + temp_min + " " + control_vars.to + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) )+ " " + temp_max);
                    if(parent.hasClass('wpestate-price-popoup-wrapper')){
                        component_price_min.val( wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) + " " + temp_min );
                        component_price_max.val( wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol) ) + " " + temp_max );
                        component_label_value = ""+ wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) + " " + wpestate_formatNumber_short(ui.values[0])+ " - " +  wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) + " " + wpestate_formatNumber_short(ui.values[1]) ;
                    
                        dropdown_parent.text(component_label_value);
                        dropdown_label.val(component_label_value);
                    }
               
                } else {
                    jQuery("#" + amount).text(temp_min + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) )+ " " + control_vars.to + " " + temp_max+ " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) );
                    if(parent.hasClass('wpestate-price-popoup-wrapper')){
                        component_price_min.val( temp_min + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) );
                        component_price_max.val( temp_max + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol)  ) );
                        component_label_value= wpestate_formatNumber_short(ui.values[0]) + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) + " - " + wpestate_formatNumber_short(ui.values[1]) + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) ;
                   
                        dropdown_parent.text(component_label_value);
                        dropdown_label.val(component_label_value);
                    }
                }
            } else {
                slider_parent.find("#" + price_low).val(ui.values[0]);
                slider_parent.find("#" + price_max).val(ui.values[1]);


                jQuery("#price_low").val(ui.values[0]);
                jQuery("#price_max").val(ui.values[1]);

                if (control_vars.where_curency === 'before') {
                    jQuery("#" + amount).text( wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + temp_min + " " + control_vars.to + " " +  wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + temp_max );
                    if(parent.hasClass('wpestate-price-popoup-wrapper')){
                        component_price_min.val( wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + temp_min );
                        component_price_max.val( wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + temp_max );
                        component_label_value = ""+ wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + wpestate_formatNumber_short(ui.values[0])+ " - " +  wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + wpestate_formatNumber_short(ui.values[1])  ;
                  
                        dropdown_parent.text(component_label_value);
                        dropdown_label.val(component_label_value);
                  
                    }
                } else {
                    jQuery("#" + amount).text(temp_min + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + control_vars.to + " " + temp_max + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) );
                    if(parent.hasClass('wpestate-price-popoup-wrapper')){
                        component_price_min.val( temp_min + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) );
                        component_price_max.val( temp_max + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) )  );
                        component_label_value = wpestate_formatNumber_short(ui.values[0]) + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " - " + wpestate_formatNumber_short(ui.values[1]) + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) ;
                   
                        dropdown_parent.text(component_label_value);
                        dropdown_label.val(component_label_value);
                    }
                }
            }
        }
    });
}





/*
*
* replace string
*
*/
function wpestate_replace_plus(string){
    return string.replace("+"," ");
}

/*
*
* js indiam format
*
*/

function wpstrea_js_indian_format(value){
    value=value.toString();
    var afterPoint = '';
    if(value.indexOf('.') > 0)
       afterPoint = value.substring(value.indexOf('.'),value.length);
    value = Math.floor(value);
    value=value.toString();
    var lastThree = value.substring(value.length-3);
    var otherNumbers = value.substring(0,value.length-3);
    if(otherNumbers != '')
        lastThree = ',' + lastThree;
    var res = otherNumbers.replace(/\B(?=(\d{2})+(?!\d))/g, ",") + lastThree + afterPoint;
    return res;
}


/*
*
* Eanble Slider
*
*/
function wpestate_enable_slider(slider_name, price_low, price_max, amount, my_custom_curr_pos, my_custom_curr_symbol, my_custom_curr_cur_post, my_custom_curr_coef) {

    var price_low_val, price_max_val, temp_min, temp_max, slider_min, slider_max;
    price_low_val = parseInt(jQuery('#'+price_low).val(), 10);
    price_max_val = parseInt(jQuery('#'+price_max).val(), 10);


    slider_min = control_vars.slider_min;
    slider_max = control_vars.slider_max;
    

    if( jQuery('#'+price_max).hasClass('adv_search_elementor_price_max') ){
        slider_max = jQuery('#'+price_max).attr('data-value');
        slider_min = jQuery('#'+price_low).attr('data-value');       
    }

    if( jQuery('#'+price_max).hasClass('wpestate_slider_in_tab') ){
        slider_max = jQuery('#'+price_max).attr('data-value');
        slider_min = jQuery('#'+price_low).attr('data-value');
       
    }



    if (!isNaN(my_custom_curr_pos) && my_custom_curr_pos !== -1) {
        slider_min =slider_min *my_custom_curr_coef;
        slider_max =slider_max *my_custom_curr_coef;
    }
    
    var parent =  jQuery("#" + slider_name).parent().parent();
    var component_price_min;
    var component_price_max;
    var dropdown_parent;
    var dropdown_label;
    var component_label_value;
    if(parent.hasClass('wpestate-price-popoup-wrapper')){
        component_price_min=parent.find('#component_'+price_low);
        component_price_max=parent.find('#component_'+price_max);
        var dropdown_parent=parent.parent().find('.dropdown-toggle');
        dropdown_label = parent.find('.price_label_component');
    
    }


    jQuery("#" + slider_name).slider({
        range: true,
        min: parseFloat(slider_min),
        max: parseFloat(slider_max),
        values: [price_low_val, price_max_val ],
        slide: function (event, ui) {

            jQuery("#" + price_low).val(ui.values[0]);
            jQuery("#" + price_max).val(ui.values[1]);
            temp_min= ui.values[0].format() ;
            temp_max= ui.values[1].format();
            if(control_vars.indian_format==="yes"){
              temp_min=wpstrea_js_indian_format(ui.values[0]);
              temp_max=wpstrea_js_indian_format(ui.values[1]);
            }

            if (!isNaN(my_custom_curr_pos) && my_custom_curr_pos !== -1) {
                if (my_custom_curr_cur_post === 'before') {
                    jQuery("#" + amount).text( wpestate_replace_plus( decodeURIComponent ( my_custom_curr_symbol ) ) + " " + temp_min + " " + control_vars.to + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) )+ " " + temp_max);
                    if(parent.hasClass('wpestate-price-popoup-wrapper')){
                        component_price_min.val( wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) + " " + temp_min );
                        component_price_max.val( wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol) ) + " " + temp_max );
                        component_label_value = ""+ wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) + " " + wpestate_formatNumber_short(ui.values[0])+ " - " +  wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) + " " + wpestate_formatNumber_short(ui.values[1]) ;
                    
                        dropdown_parent.text(component_label_value);
                        dropdown_label.val(component_label_value);
                    }
                } else {
                    jQuery("#" + amount).text(temp_min + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) )+ " " + control_vars.to + " " + temp_max + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) );
                    if(parent.hasClass('wpestate-price-popoup-wrapper')){
                        component_price_min.val( temp_min + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) );
                        component_price_max.val( temp_max + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol)  ) );
                        component_label_value= wpestate_formatNumber_short(ui.values[0]) + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) + " - " + wpestate_formatNumber_short(ui.values[1]) + " " + wpestate_replace_plus ( decodeURIComponent ( my_custom_curr_symbol ) ) ;
                   
                        dropdown_parent.text(component_label_value);
                        dropdown_label.val(component_label_value);
                    }
                }
            } else {

                if (control_vars.where_curency === 'before') {
                    jQuery("#" + amount).text(""+ wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + temp_min+ " " + control_vars.to + " " +  wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + temp_max);
                    if(parent.hasClass('wpestate-price-popoup-wrapper')){
                        component_price_min.val( wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + temp_min );
                        component_price_max.val( wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + temp_max );
                        component_label_value = ""+ wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + wpestate_formatNumber_short(ui.values[0])+ " - " +  wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + wpestate_formatNumber_short(ui.values[1])  ;
                  
                        dropdown_parent.text(component_label_value);
                        dropdown_label.val(component_label_value);
                  
                    }
                } else {
                    jQuery("#" + amount).text(temp_min + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " " + control_vars.to + " " + temp_max + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) );
                    if(parent.hasClass('wpestate-price-popoup-wrapper')){
                        component_price_min.val( temp_min + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) );
                        component_price_max.val( temp_max + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) )  );
                        component_label_value = wpestate_formatNumber_short(ui.values[0]) + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) + " - " + wpestate_formatNumber_short(ui.values[1]) + " " + wpestate_replace_plus ( decodeURIComponent ( control_vars.curency ) ) ;
                   
                        dropdown_parent.text(component_label_value);
                        dropdown_label.val(component_label_value);
                    }
                }
            }
        }
    });
}

/*
*
* enable slider for elementor
*
*/

function wpestate_enable_slider_elementor(){

    jQuery('.wpestate_elementor_search_tab_slider_wrapper').each(function(){
        var slider_id = jQuery(this).find('.wpestate_elementor_search_tab_slider').attr('id');
        var price_min = jQuery(this).find('.adv_search_elementor_price_low').attr('id');
        var price_max = jQuery(this).find('.adv_search_elementor_price_max').attr('id');
        var ammount   = jQuery(this).find('.wpresidence_slider_price').attr('id');
        var my_custom_curr_symbol  =   decodeURI ( wpestate_getCookie('my_custom_curr_symbol') );
        var my_custom_curr_coef    =   parseFloat( wpestate_getCookie('my_custom_curr_coef'));
        var my_custom_curr_pos     =   parseFloat( wpestate_getCookie('my_custom_curr_pos'));
        var my_custom_curr_cur_post=   wpestate_getCookie('my_custom_curr_cur_post');
        wpestate_enable_slider(slider_id, price_min, price_max, ammount, my_custom_curr_pos, my_custom_curr_symbol, my_custom_curr_cur_post,my_custom_curr_coef);

    });
    
}

/*
*
* 
*
*/
function wpestate_formatNumber_short(num) {
    num=parseFloat(num);
 
    if (num >= 1000000000) {
        return (num / 1000000000).toFixed(1).replace(/\.0$/, '') + 'B';
    }
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
    }
    if (num >= 1000) {
        return (num / 1000).toFixed(1).replace(/\.0$/, '') + 'K';
    }
    return num.toString();
}

/*
*
* 
*
*/


function wpestate_done_typing(){
    if (typeof (wpestate_show_pins) !== "undefined") {
        first_time_wpestate_show_inpage_ajax_half=1;

        wpestate_show_pins();
    }
}


/*
*
* 
*
*/
function isFunction(possibleFunction) {
     return typeof(possibleFunction) === typeof(Function);
}



/*
*
* calculate morgage widget action
*/

function wpestate_zillow_widget_action(){
    
    jQuery('#zill_submit_estimate').on( 'click', function(event) {
        var button = jQuery(this);
        var container = button.parent();
        var zillow_adress = jQuery('#zill_estimate_adr1').val();
        var zillow_city = jQuery('#zill_estimate_city1').val();
        var zillow_state = jQuery('#zill_estimate_state1').val();

        var full_address = zillow_adress+', '+zillow_city+', '+zillow_state;

        if(zillow_adress==''|| zillow_city=='' || zillow_state==''){
            var answer=control_vars.zillow_fields;
            container.find('.wpestate_zillow_answer').empty().append(answer);
            return;
        }



        container.find('.wpestate_zillow_answer').empty().append(control_vars.zillow_wait);

        ajaxurl     =   ajaxcalls_vars.admin_url + 'admin-ajax.php';
        var nonce = jQuery('#wpestate_zillow_nonce').val();
        jQuery.ajax({
            type: 'POST',
            dataType: 'json',
            url: ajaxurl,
            data: {
                'action'    :   'wpestate_zillow_estimation',
                'zillow_adress'      :   zillow_adress,
                'zillow_city'       :   zillow_city,
                'zillow_state'    :   zillow_state,
                'full_address'      :   full_address,
                'security'  :   nonce
            },
            success: function (data) {
              
                
                
                
                if(data.total===0){
                    var answer=control_vars.zillow_none+" "+full_address;
                }else{

                    var zillow_price = data.bundle[0].zestimate;
                  
                    const formattedPrice = new Intl.NumberFormat('en-US', {
                        style: 'currency',
                        currency: 'USD',
                        useGrouping: true
                      }).format(zillow_price);
    
    
                    var answer =control_vars.wpestate_zillow_answer+" <span class='wpestate_zillow_address'>"+full_address+"</span> "+control_vars.zillow_is+" <span class='wpestate_zillow_price'>"+formattedPrice+"</span>";
                    
                }
                
            
                container.find('.wpestate_zillow_answer').empty().append(answer);
            
            },
            error: function (errorThrown) {}
        });//end ajax





       
    });
}

/*
*
* calculate morgage widget action
*/

function wpestate_morgate_widget_action(){
    jQuery('#morg_compute').on( 'click', function(event) {

        var intPayPer  = 0;
        var intMthPay  = 0;
        var intMthInt  = 0;
        var intPerFin  = 0;
        var intAmtFin  = 0;
        var intIntRate = 0;
        var intAnnCost = 0;
        var intVal     = 0;
        var salePrice  = 0;

        salePrice = jQuery('#sale_price').val();
        intPerFin = jQuery('#percent_down').val() / 100;

        intAmtFin = salePrice - salePrice * intPerFin;
        intPayPer =  parseInt (jQuery('#term_years').val(),10) * 12;
        intIntRate = parseFloat (jQuery('#interest_rate').val(),10);
        intMthInt = intIntRate / (12 * 100);
        intVal = wpestate_raisePower(1 + intMthInt, -intPayPer);
        intMthPay = intAmtFin * (intMthInt / (1 - intVal));
        intAnnCost = intMthPay * 12;

        jQuery('#am_fin').html("<strong>"+control_vars.morg1+"</strong> " + (Math.round(intAmtFin * 100)) / 100 + " ");
        jQuery('#morgage_pay').html("<strong>"+control_vars.morg2+"</strong>" + (Math.round(intMthPay * 100)) / 100 + " ");
        jQuery('#anual_pay').html("<strong>"+control_vars.morg3+"</strong>" + (Math.round(intAnnCost * 100)) / 100 + " ");
        jQuery('#morg_results').show();
        jQuery('.mortgage_calculator_div').css('height',532+'px');
    });

}

/*
*
* sanitzie string to be used as classname
*
*/
function wpestateSanitizeClassName(str) {
    // Remove leading numbers by replacing them with a prefix (e.g., 'class-')
    str = str.replace(/^\d+/, 'class-');

    // Replace invalid characters with a hyphen
    str = str.replace(/[^a-zA-Z0-9-_]/g, '-');

    // Remove multiple hyphens
    str = str.replace(/-+/g, '-');

    // Remove leading and trailing hyphens
    str = str.trim().replace(/(^-|-$)/g, '');

    return str;
}


/**
* Initialize autocomplete functionality for location search fields.
* 
* This function sets up jQuery UI autocomplete on location input fields with the following features:
* - Filters and displays location suggestions as user types
* - Limits results to 12 items
* - Adds a reset button after selected text
* - Groups results by location category (City, Area, County/State)
* - Updates map pins on selection
* 
* @since 4.9.0
* @param {Array} availableTags - Array of location objects with properties:
*   @param {string} category - Location category ("City", "Area", "County or State")
*   @param {string} labelcategory - Category identifier for CSS ("city", "area", "countystate")
*   @param {string} label - Display name of the location
*   @param {string} value - Location value to be used
* @param {string} inputID - ID of the input field to initialize autocomplete on
* 
* @requires jQuery
* @requires jQuery UI Autocomplete
* 
* DOM Requirements:
* - Input field must be wrapped in .adv_locations_search_component_wrapper
* - Input field must have class .adv_locations_search
* 
* Global variables used:
* - first_time_wpestate_show_inpage_ajax_half
* 
* Related functions:
* - wpestate_show_pins() - Updates map pins after selection
*/
function wpresidenceInitializeAutocomplete(availableTags, inputID) {
    jQuery("#" + inputID).autocomplete({
        source: function(request, response) {
            var results = jQuery.ui.autocomplete.filter(availableTags, request.term);
            response(results.slice(0, 12));
        },
        select: function(event, ui) {
            var $input = jQuery(this);
            var $wrapper = $input.parent();
            var selectedText = ui.item.label; // Get the selected text
            
            // Create reset button
            var $resetBtn = jQuery(`
                <button type="button" class="wpresidece-reset-btn">
                    <svg width="14px" height="14px" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <g id="SVGRepo_bgCarrier" stroke-width="0"></g>
                        <g id="SVGRepo_tracerCarrier" stroke-linecap="round" stroke-linejoin="round"></g>
                        <g id="SVGRepo_iconCarrier">
                            <rect width="24" height="24"></rect>
                            <path d="M7 17L16.8995 7.10051" stroke="#000000" stroke-linecap="round" stroke-linejoin="round"></path>
                            <path d="M7 7.00001L16.8995 16.8995" stroke="#000000" stroke-linecap="round" stroke-linejoin="round"></path>
                        </g>
                    </svg>
                </button>`);

            // Remove any existing reset button first
            $wrapper.find('.wpresidece-reset-btn').remove();
            
            // Add the reset button after input
            $input.after($resetBtn);

            // Calculate position based on selected text
            var $measure = jQuery('<span>').css({
                'font-family': $input.css('font-family'),
                'font-size': $input.css('font-size'),
                'font-weight': $input.css('font-weight'),
                'position': 'absolute',
                'visibility': 'hidden',
                'white-space': 'pre'
            }).text(selectedText);
            
            $wrapper.append($measure);
            
            var inputPadding = parseInt($input.css('padding-left'));
            var textWidth = $measure.width();
            
            $resetBtn.css({
                'left': (inputPadding + textWidth + 5) + 'px', // 5px buffer 
                'right': 'auto'
            });
            
            $measure.remove();

            // Event handler for the reset button
            $resetBtn.on('click', function() {
                $input.val("");
                jQuery(this).remove();
                setTimeout(function() {
                    first_time_wpestate_show_inpage_ajax_half = 1;
                    wpestate_show_pins();
                }, 1000);
            });

            setTimeout(function() {
                first_time_wpestate_show_inpage_ajax_half = 1;
                wpestate_show_pins();
            }, 1000);
        },
        change: function() {
        }
    }).autocomplete("instance")._renderItem = function(ul, item) {
        var item_class = "wpresidence-geolocatiomarker-" + item.labelcategory;
        var $li = jQuery("<li>").append("<div class='" + item_class + "'>" + item.label + "</div>");

        var lastCategory = jQuery(ul).find(".wpresidece-dropdown-category-label:last").data("category");
        if (lastCategory !== item.category) {
            var $categoryDiv = jQuery("<div>")
                .addClass("wpresidece-dropdown-category-label")
                .data("category", item.category)
                .text(item.category);
            jQuery(ul).append($categoryDiv);
        }

        return $li.appendTo(ul);
    };
}



/*
*
* save search actions
*
*/
function wpestate_save_search_actions(){

    jQuery('#save_search_button').on( 'click', function(event) {
        var nonce, search, search_name, parent, ajaxurl,meta;
        search_name     =   jQuery('#search_name').val();
        search          =   jQuery('#search_args').val();
        meta            =   jQuery('#meta_args').val();
        nonce           =   jQuery('#wpestate_save_search_nonce').val();
        ajaxurl         =   ajaxcalls_vars.admin_url + 'admin-ajax.php';
        jQuery('#save_search_notice').html(control_vars.save_search);

        jQuery.ajax({
            type: 'POST',
            url: ajaxurl,
            data: {
                'action'        :   'wpestate_save_search_function',
                'search_name'   :   search_name,
                'search'        :   search,
                'meta'          :   meta,
                'security'         :   nonce
            },
            success: function (data) {

                jQuery('#save_search_notice').html(data);
                jQuery('#search_name').val('');
            },
            error: function (errorThrown) {
            }
        });

    });
}

/*
*
* delete save search actions
*
*/
function wpestate_delete_save_search_actions(){
    jQuery('.delete_search').on( 'click', function(event) {
        var  search_id, parent, ajaxurl,confirmtext;
        confirmtext = control_vars.deleteconfirm;

        if (confirm(confirmtext)) {
            event.preventDefault();
            ajaxurl         =   ajaxcalls_vars.admin_url + 'admin-ajax.php';
            search_id       =   jQuery(this).attr('data-searchid');
            parent          =   jQuery(this).parent();
            jQuery(this).html(control_vars.deleting);
            var nonce = jQuery('#wpestate_searches_actions').val();
            jQuery.ajax({
                type: 'POST',
                url: ajaxurl,
                data: {
                    'action'        :   'wpestate_delete_search',
                    'search_id'     :   search_id,
                    'security'      :   nonce,
                },
                success: function (data) {

                    if (data==='deleted'){
                        parent.remove();
                    }

                },
                error: function (errorThrown) {
                }
            });


        }

    });
}



/*
*
* 
*
*/
function wpestate_overview_map(){
    jQuery('#overview_map').click(function(){
        jQuery('#wpestate_overview_map_modal').appendTo("body");
        jQuery('body> #wpestate_overview_map_modal').first().modal("show");
        
    });
}









////////////////

/**
 * Creates skeleton cards in container based on existing card count
 * @param {jQuery} container - Container element
 * @param {boolean} append - If true, append skeletons; if false, empty container first (default: false)
 * 
 * data-row-number="3" - blg list
 */
function wpestate_createSkeletons(container, append) {
    // Default parameter handling for ES5 compatibility
    if (typeof append === 'undefined') {
        append = false;
    }
    
     // Detect if this is a grid layout
    var isGrid = container.find('.shortcode_wrapper_grid_item').length > 0;
    
    // Count existing cards BEFORE they get cleared
    var existingCards = container.find('.listing_wrapper:not(.skeleton-placeholder)');
    var existingCount = existingCards.length;
    
    // Handle clearing based on append parameter
    if (append === false) {
        container.empty();
    }
   
    // Calculate items per row from Bootstrap grid classes based on current viewport
    var itemsPerRow = parseInt( container.attr('data-row-number')) ;
    var gridClasses = 'col-lg-4 col-12 col-sm-6 col-md-6';
    var cardClasses = 'property_listing property_card_default';
   


    if (isGrid) {
        // Grid layout logic

        // For grid, we don't use Bootstrap classes
        gridClasses = '';
        
        // Extract card classes from existing cards
        if (existingCards.length > 0) {
            var cardElement = existingCards.first().find('.property_listing');
            if (cardElement.length > 0) {
                cardClasses = cardElement.attr('class');
            }
        }
        
        
        
    } else {
            
        if (existingCards.length > 0) {
            var firstCard = existingCards.first();
            var classList = firstCard.attr('class').split(' ');
        
            // Extract grid classes
            var extractedGridClasses = classList.filter(function(cls) {
                return cls.indexOf('col-') === 0;
            }).join(' ');
            if (extractedGridClasses) {
                gridClasses = extractedGridClasses;
            }
        
            // Calculate items per row based on current viewport
            var viewportWidth = window.innerWidth;
            var cols = 4; // default (col-lg-4 = 12/4 = 3 per row)
        
            if (viewportWidth >= 1200) {
                // xl breakpoint - look for col-xl-* or fallback to col-lg-*
                var xlClass = classList.find(function(cls) { return cls.match(/^col-xl-(\d+)$/); });
                if (xlClass) {
                    cols = parseInt(xlClass.match(/^col-xl-(\d+)$/)[1]);
                } else {
                    var lgClass = classList.find(function(cls) { return cls.match(/^col-lg-(\d+)$/); });
                    if (lgClass) cols = parseInt(lgClass.match(/^col-lg-(\d+)$/)[1]);
                }
            } else if (viewportWidth >= 992) {
                // lg breakpoint
                var lgClass = classList.find(function(cls) { return cls.match(/^col-lg-(\d+)$/); });
                if (lgClass) cols = parseInt(lgClass.match(/^col-lg-(\d+)$/)[1]);
            } else if (viewportWidth >= 768) {
                // md breakpoint
                var mdClass = classList.find(function(cls) { return cls.match(/^col-md-(\d+)$/); });
                if (mdClass) cols = parseInt(mdClass.match(/^col-md-(\d+)$/)[1]);
            } else if (viewportWidth >= 576) {
                // sm breakpoint
                var smClass = classList.find(function(cls) { return cls.match(/^col-sm-(\d+)$/); });
                if (smClass) cols = parseInt(smClass.match(/^col-sm-(\d+)$/)[1]);
            } else {
                // xs breakpoint - look for col-* (no breakpoint)
                var xsClass = classList.find(function(cls) { return cls.match(/^col-(\d+)$/); });
                if (xsClass) cols = parseInt(xsClass.match(/^col-(\d+)$/)[1]);
            }
        
            itemsPerRow = Math.floor(12 / cols);
        
            // Extract card classes
            var cardElement = firstCard.find('.property_listing');
            if (cardElement.length > 0) {
                cardClasses = cardElement.attr('class');
            }
        }
    }

    var skeletonCount;
    
    // Simple: skeleton count = existing count (complete rows)
    if (append === false) {
        skeletonCount = Math.ceil(existingCount / itemsPerRow) * itemsPerRow;
    }else{
         

        if(isGrid){
            skeletonCount = parseInt( container.parent().attr('data-row-number'));
        }else{
            skeletonCount = itemsPerRow;
          }
    }

    // Create skeleton HTML
    var skeletonHTML = '';
    for (var i = 0; i < skeletonCount; i++) {
        skeletonHTML += wpestate_createSkeletonCard(gridClasses, cardClasses, false);
    }
   
    // Clear specific elements if appending (don't clear everything)
   
   
    // Add skeletons to container
    container.append(skeletonHTML);
}

/**
 * Replaces skeleton cards with real cards
 * @param {jQuery} container - Container element
 * @param {string} htmlData - HTML data with real cards
 */
function wpestate_replaceSkeletons(container, htmlData,append) {


    // Detect if this is a grid layout
    var isGrid = container.find('.shortcode_wrapper_grid_item').length > 0;

    // Default parameter handling for ES5 compatibility
    if (typeof append === 'undefined') {
        append = false;
    }
    
    // Find skeleton placeholders
    var skeletons = container.find('.skeleton-placeholder');
    console.log('wpestate_replaceSkeletons: skeleton placeholders to replace', skeletons.length);
    // Parse new HTML data
    var tempDiv = jQuery('<div>').html(htmlData);
    var newCards = tempDiv.find('.listing_wrapper');
    console.log('wpestate_replaceSkeletons: listings found in htmlData', newCards.length);
    var pagination = tempDiv.find('.pagination_ajax_search');
    var pagination2 = tempDiv.find('.pagination_ajax');
    var scrollHere = tempDiv.find('#scrollhere');
    var no_results = tempDiv.find('.no_results');
  
    // If no skeletons, just append normally
    if (skeletons.length === 0) {
        container.append(htmlData);
        return;
    }
    
    // Add scrollhere at beginning if exists
    if (scrollHere.length > 0) {
        container.prepend(scrollHere);
    }
    
    // Replace ALL skeletons one by one - some with real cards, others removed
    var delay = 0;
    var intervalTime = 100;
    
    var maxItems = Math.max(skeletons.length, newCards.length);
    if (append == true) {
        console.log('maxItems with app ' + maxItems);
    } else {
        console.log('maxItems no app ' + maxItems);
    }


    for (var i = 0; i < maxItems; i++) {
        
        (function(index) {
            setTimeout(function() {
                if (index < skeletons.length && index < newCards.length) {
                    // Replace skeleton with real card
                    
                    if(isGrid){
                        var wrappedCard = jQuery('<div class="shortcode_wrapper_grid_item"></div>').append(jQuery(newCards[index]));
                        jQuery(skeletons[index]).replaceWith(wrappedCard);
                    }else{
                        jQuery(skeletons[index]).replaceWith(jQuery(newCards[index]));
                    }
                


                } else if (index < skeletons.length) {
                    // Remove empty skeleton (right to left)
                    var emptySkeletonIndex = skeletons.length - 1 - (index - newCards.length);
                    jQuery(skeletons[emptySkeletonIndex]).fadeOut(200, function() {
                        jQuery(this).remove();
                    });
                } else if (index < newCards.length) {
                    // Add extra card (more cards than skeletons)
                    container.append(jQuery(newCards[index]));
                }
            }, delay);
        })(i);
        
        delay += intervalTime;

    } 

    
    // Add pagination after all skeletons are processed
    if (pagination.length > 0) {
        setTimeout(function() {
            container.append(pagination);
        }, delay + 200);
    }
    

  // Add pagination2 after all skeletons are processed
    if (pagination2.length > 0) {
        setTimeout(function() {
            container.append(pagination2);
        }, delay + 200);
    }
   
     // Add pagination2 after all skeletons are processed
    if (no_results.length > 0) {
        setTimeout(function() {
            container.append(no_results);
        }, delay + 200);
    }


     // Call restart JS after all real cards are displayed
    setTimeout(function() {
        if (typeof wpestate_restart_js_after_ajax === 'function') {
            wpestate_restart_js_after_ajax();
        }
    }, delay + 300);

}









/**
 * Creates a skeleton placeholder card
 */
function wpestate_createSkeletonCard(gridClasses, cardClasses, is_grid) {
    var skeletonCard = `
    <div class="${gridClasses} listing_wrapper skeleton-placeholder">
        <div class="${cardClasses} skeleton-card">
            <div class="listing-unit-img-wrapper">
                <div class="skeleton-image skeleton-shimmer"></div>
            </div>
            <div class="property-unit-information-wrapper">
                <div class="skeleton-text title skeleton-shimmer"></div>
                <div class="skeleton-text category skeleton-shimmer"></div>
                <div class="skeleton-text details skeleton-shimmer"></div>
            </div>
        </div>
    </div>`;
    
    if (is_grid) {
        return `<div class="shortcode_wrapper_grid_item">${skeletonCard}</div>`;
    }
    
    return skeletonCard;
}

/*global $, jQuery,wpestate_login_register_vars, ajaxcalls_vars, document, control_vars, window*/


jQuery(document).ready(function ($) { 
    "use strict";
     
    jQuery('#login-modal_close').on( 'click', function(event) {
        jQuery('#modal_login_wrapper').hide();
    });
    wpestate_trigger_login_action();
    wpestate_trigger_register_action();
    wpestate_trigger_forgot_password_action();
    wpestate_controls_show_hide_login_register_forgot_form();
    wpestate_initializePasswordVisibilityToggle();
    wpestate_handle_social_login();
    wpestate_handle_password_change();
})  

/**  
 * reCAPTCHA Initialization Function
 * 
 * This function initializes reCAPTCHA widgets for all elements with the class
 * 'wpestate_register_form_captcha'. It's designed to be used as a callback
 * when the reCAPTCHA script is loaded.
 *
 * @global {Array} widgetRecaptchas - Stores the IDs of all rendered reCAPTCHA widgets
 * @global {Object} control_vars - Expected to contain the reCAPTCHA site key
 * 
 * Usage:
 * 1. Ensure the reCAPTCHA script is loaded with this function as a callback
 * 2. Add the class 'wpestate_register_form_captcha' to any element where you want a reCAPTCHA widget
 * 
 * Note: This function assumes that 'control_vars.captchakey' contains a valid reCAPTCHA site key
 */

var widgetRecaptchas = [];
var wpestate_onloadCallback = function() {
    // Find all elements with the class 'wpestate_register_form_captcha'
    var captchaElements = document.getElementsByClassName('wpestate_register_form_captcha');

    // Render reCAPTCHA for each element found
    for (var i = 0; i < captchaElements.length; i++) {
        var widgetRecaptcha = grecaptcha.render(captchaElements[i], {
            'sitekey': control_vars.captchakey,
            'theme': 'light'
        });
        widgetRecaptchas.push(widgetRecaptcha);
    }
};




/**
 * Handle password change events
 * 
 * This function sets up event handlers for password change functionality.
 * It handles both Enter key presses in password fields and clicks on the
 * change password button.
 *
 * @since 1.0.0
 * 
 * @requires jQuery
 * @requires wpestate_change_pass_profile function (assumed to exist)
 * 

 */ 



function wpestate_handle_password_change() {
    const ENTER_KEY_CODE = 13;
    const passwordFields = '#oldpass, #newpass, #renewpass';
    const changePasswordButton = '#change_pass';

    // Handle Enter key press in password fields
    jQuery(passwordFields).on('keydown', function(event) {
        if (event.keyCode === ENTER_KEY_CODE) {
            event.preventDefault();
            wpestate_change_pass_profile();
        } 
    }); 
   
    // Handle click on change password button
    jQuery(changePasswordButton).on('click', function(event) {
        event.preventDefault();
        wpestate_change_pass_profile();
    });
}


function wpestate_change_pass_profile() {
    "use strict";
    var oldpass, newpass, renewpass, securitypass, ajaxurl;
    oldpass = jQuery('#oldpass').val();
    newpass = jQuery('#newpass').val();
    renewpass = jQuery('#renewpass').val();
    securitypass = jQuery('#security-pass').val();
    ajaxurl = ajaxcalls_vars.admin_url + 'admin-ajax.php';

    jQuery.ajax({
        type: 'POST',
        url: ajaxurl,
        data: {
            'action': 'wpestate_ajax_update_pass',
            'oldpass': oldpass,
            'newpass': newpass,
            'renewpass': renewpass,
            'security-pass': securitypass
        },
        success: function (data) {
            jQuery('#profile_pass').empty().append('<div class="login-alert">' + data + '<div>');
            jQuery('#oldpass, #newpass, #renewpass').val('');
        },
        error: function (errorThrown) {
        }
    });
}





/**
 * Handle social login button clicks
 * 
 * This function sets up a click event handler for social login buttons.
 * When clicked, it sends an AJAX request to generate a social login link
 * and redirects the user to the generated URL.
 *
 * @since 1.0.0
 * 
 * @requires jQuery
 * @requires ajaxcalls_vars.admin_url
 * 
 * @example
 * // Call this function when the document is ready
 * $(document).ready(function() {
 *     wpestate_handle_social_login();
 * });
 */
function wpestate_handle_social_login() {
    jQuery('.wpestate_social_login').on('click', function(event) {
        event.preventDefault(); // Prevent default button action

        // Get the AJAX URL from the global variable
        var ajaxUrl = ajaxcalls_vars.admin_url + 'admin-ajax.php';

        // Extract social login type and nonce from the clicked button
        var socialLoginType = jQuery(this).attr('data-social');
        var socialLoginNonce = jQuery(this).parent().find('.wpestate_social_login_nonce').val();

        // Send AJAX request to generate social login link
        jQuery.ajax({
            type: 'POST',
            url: ajaxUrl,
            data: {
                'action': 'wpestate_social_login_generate_link',
                'social_type': socialLoginType,
                'nonce': socialLoginNonce
            },
            success: function(response) {
                // Redirect to the generated social login URL
                window.location.href = response;
            },
            error: function(jqXHR, textStatus, errorThrown) {
                // Log error for debugging
                console.error('Social login error:', textStatus, errorThrown);
                // Optionally, display an error message to the user
                alert('An error occurred during social login. Please try again.');
            }
        });
    });
}



/**
 * Controls the visibility of login, register, and forgot password forms.
 * 
 * This function sets up click event handlers for form switching buttons
 * in the WpResidence theme's login widget. It manages the visibility of
 * different form sections (login, register, forgot password) and their
 * corresponding switch buttons.
 * 
 * @since 1.0.0
 * @requires jQuery
 * 
 * Usage:
 * - Call this function after the DOM is ready.
 * - Ensure that the form structure follows the expected HTML layout:
 *   - Forms should have classes: wpestate_login_form_wrapper, wpestate_register_form_wrapper, wpestate_forgot_form_wrapper
 *   - Switch buttons should have classes: wpestate_login_form_switch_login, wpestate_login_form_switch_register, wpestate_login_form_switch_forgot
 * 
 * @return {void}
 */
function wpestate_controls_show_hide_login_register_forgot_form() {

    jQuery('.wpestate_login_form_switch_login, .wpestate_login_form_switch_register, .wpestate_login_form_switch_forgot').on('click', function (event) {
        event.preventDefault();
        var $this = jQuery(this);
        var form_wrapper = $this.parent().parent();
        var clicked_type = $this.attr('class').split('_').pop();

        // Hide all forms and show all switches
        form_wrapper.find('.wpestate_login_form_wrapper, .wpestate_register_form_wrapper, .wpestate_forgot_form_wrapper').hide();
        form_wrapper.find('.wpestate_login_form_switch_login, .wpestate_login_form_switch_register, .wpestate_login_form_switch_forgot').show();

        // Show the clicked form and hide its switch
        form_wrapper.find('.wpestate_' + clicked_type + '_form_wrapper').show().find('[autofocus]').focus();
        $this.hide();
    });
}



/**
 * Toggles password visibility for input fields.
 * 
 * This function sets up a click event handler for elements with the class 'show_hide_password'.
 * When clicked, it toggles the visibility of the adjacent password input field and updates
 * the icon class accordingly.
 *
 * @requires jQuery
 * 
 * Usage:
 * - Call this function after the DOM is ready.
 * - Ensure that the HTML structure follows the expected layout:
 *   - A container element with a child element having the class 'show_hide_password'
 *   - An input field (type="password") as a sibling to the 'show_hide_password' element
 * 
 * HTML structure example:
 * <div class="password-container">
 *   <input type="password" name="password" />
 *   <i class="show_hide_password fa fa-eye-slash"></i>
 * </div>
 * 
 * @return {void}
 */
function wpestate_initializePasswordVisibilityToggle() {
    jQuery('.show_hide_password').on('click', function() {
        // Find the password input field within the same parent container
        var passField = jQuery(this).parent().find(':input');
        
        if (passField.attr('type') === "password") {
            // If the field is a password, change it to text and update the icon
            passField.attr('type', 'text');
            jQuery(this).removeClass('fa-eye-slash').addClass('fa-eye');
        } else {
            // If the field is text, change it back to password and update the icon
            passField.attr('type', 'password');
            jQuery(this).removeClass('fa-eye').addClass('fa-eye-slash');
        }
    });
}



/**
 * Sets up event handlers for user registration actions.
 * 
 * This function initializes click and keydown event handlers for the registration
 * form elements in the WpResidence theme. It handles both button clicks and
 * Enter key presses on input fields to trigger the user registration process.
 *
 * @requires jQuery
 * @requires wpestate_register_user function (not defined here)
 * 
 * Usage:
 * - Call this function after the DOM is ready and after defining wpestate_register_user.
 * - Ensure that the HTML structure follows the expected layout:
 *   - Registration form wrapper with class 'wpestate_register_form_wrapper'
 *   - Submit button with class 'wpestate_register_submit_button'
 *   - Input fields with classes 'wpestate_register_form_email', 'wpestate_register_form_username',
 *     'wpestate_register_form_password', 'wpestate_register_form_user_type'
 * 
 * @return {void}
 */
function wpestate_trigger_register_action() {
    const $document = jQuery(document);
    const ENTER_KEY_CODE = 13;

    // Handle click on register submit button
    $document.on('click', '.wpestate_register_submit_button', function(event) {
        wpestate_register_user(jQuery(this));
    });

    // Handle Enter key press on registration form fields
    $document.on('keydown', '.wpestate_register_form_email, .wpestate_register_form_username, .wpestate_register_form_password, .wpestate_register_form_user_type', function(event) {
        if (event.keyCode === ENTER_KEY_CODE) {
            event.preventDefault();
            const $submitButton = jQuery(this).closest('.wpestate_register_form_wrapper').find('.wpestate_register_submit_button');
            wpestate_register_user($submitButton);
        }
    });
}




/**
 * Retrieves the reCAPTCHA response for a specific form.
 *
 * This function is designed to work with multiple reCAPTCHA instances on a single page.
 * It uses the global `widgetRecaptchas` array to identify the correct reCAPTCHA widget
 * associated with the given form wrapper.
 *
 * @param {HTMLElement} formWrapper - The DOM element wrapping the form containing the reCAPTCHA.
 *                                    This should be a parent element that contains the
 *                                    .wpestate_register_form_captcha element.
 *
 * @returns {string|null} The reCAPTCHA response string if successful, or null if:
 *                        - reCAPTCHA is not enabled
 *                        - The reCAPTCHA element is not found in the form
 *                        - The corresponding widget ID cannot be found
 *                        - The reCAPTCHA response is empty
 *
 * @requires grecaptcha - The global reCAPTCHA object provided by Google's reCAPTCHA API.
 * @requires control_vars.usecaptcha - A global variable indicating whether reCAPTCHA is enabled.
 * @requires widgetRecaptchas - A global array containing the widget IDs of rendered reCAPTCHAs.
 *
 * @throws {ConsoleWarning} If the reCAPTCHA element is not found or the widget ID cannot be determined.
 *
 * @example
 * const formWrapper = document.querySelector('.my-form-wrapper');
 * const recaptchaResponse = getRecaptchaResponse(formWrapper);
 * if (recaptchaResponse) {
 *     // Proceed with form submission
 * } else {
 *     // Handle missing reCAPTCHA response
 * }
 */

function getRecaptchaResponse(formWrapper) {
    // Check if reCAPTCHA is enabled
    if (typeof grecaptcha === 'undefined' || 
        !grecaptcha || 
        control_vars.usecaptcha !== 'yes') {
        return null;
    }

    // Find the reCAPTCHA element within the form wrapper
    const recaptchaElement = formWrapper.querySelector('.wpestate_register_form_captcha');
    if (!recaptchaElement) {
        console.warn('reCAPTCHA element not found in the form');
        return null;
    }

    // Find the index of this reCAPTCHA element
    const captchaElements = document.getElementsByClassName('wpestate_register_form_captcha');
    const index = Array.from(captchaElements).indexOf(recaptchaElement);

    if (index === -1 || index >= widgetRecaptchas.length) {
        console.warn('Could not find corresponding widget ID for this reCAPTCHA');
        return null;
    }

    // Get the reCAPTCHA response using the widget ID
    const response = grecaptcha.getResponse(widgetRecaptchas[index]);

    if (!response) {
        console.warn('reCAPTCHA response is empty');
    }

    return response;
}



/**
 * Handles the user registration process.
 * 
 * This function collects user input, validates it, and sends an AJAX request
 * to register a new user in the WpResidence theme.
 *
 * @param {jQuery} $submitButton - The jQuery object of the submit button that triggered the registration.
 * @requires jQuery
 * @requires ajaxcalls_vars global object
 * @requires control_vars global object
 * @requires grecaptcha (if control_vars.usecaptcha is 'yes')
 * 
 * Expected HTML structure:
 * - Form wrapper with class 'wpestate_register_form_wrapper'
 * - Input fields with specific classes for username, email, password, etc.
 * - Terms agreement checkbox
 * - Message area for displaying status messages
 * 
 * @return {void}
 */
function wpestate_register_user($submitButton) {
    "use strict";
    
    const $formWrapper = $submitButton.closest('.wpestate_register_form_wrapper');
    const $messageArea = $formWrapper.find('.wpestate_register_message_area');
    const ajaxurl = wpestate_login_register_vars.admin_url + 'admin-ajax.php';

    // Display processing message
    $messageArea.html('<div class="login-alert">' + wpestate_login_register_vars.procesing + '</div>');
    // Collect form data
    const formData = {
        user_login_register: $formWrapper.find('.wpestate_register_form_usenmame').val(),
        user_email_register: $formWrapper.find('.wpestate_register_form_email').val(),
        security_nonce: $formWrapper.find('.wpestate_register_form_security').val(),
        user_pass: ajaxcalls_vars.userpass === 'yes' ? $formWrapper.find('.wpestate_register_form_password').val() : '',
        user_pass_retype: ajaxcalls_vars.userpass === 'yes' ? $formWrapper.find('.wpestate_register_form_password_retype').val() : '',
        new_user_type: $formWrapper.find('.wpestate_register_form_user_type').length > 0 ? $formWrapper.find('.wpestate_register_form_user_type').val() : '',
  
    };

    // Get reCAPTCHA response if it's enabled
    if (control_vars.usecaptcha === 'yes') {
         // Get reCAPTCHA response if it's enabled
         
        const recaptchaResponse = getRecaptchaResponse($formWrapper[0]);
        if (control_vars.use_captcha === 'yes' && !recaptchaResponse) {
            $messageArea.html('<div class="login-alert">Please complete the reCAPTCHA.</div>');
            return;
        }
        
        formData.capthca = recaptchaResponse;

    }
    
    
    // Validate form data
    if (!wpestate_validateRegistrationData($formWrapper, formData)) {
        return;
    }

    // Send AJAX request 
    jQuery.ajax({
        type: 'POST',
        dataType: 'json',
        url: ajaxurl,
        data: {
            action: 'wpestate_to_register_user',
            ...formData,
            type: 1
        },
        success: function(response) {
            wpestate_handleRegistrationResponse($formWrapper, $messageArea, response);
        },
        error: function(errorThrown) {
            console.error('Registration error:', errorThrown);
            $messageArea.html('<p>An error occurred during registration. Please try again.</p>');
        }
    });
}





/**
 * Validates the registration form data.
 * 
 * @param {jQuery} $formWrapper - The form wrapper element.
 * @param {Object} formData - The collected form data.
 * @return {boolean} True if data is valid, false otherwise.
 */
function wpestate_validateRegistrationData($formWrapper, formData) {
    const $messageArea = $formWrapper.find('.wpestate_register_message_area');

    if (!$formWrapper.find('.wpestate_register_form_agree_terms').is(":checked")) {
        $messageArea.html(`<div class="login-alert">${control_vars.terms_cond}</div>`);
        return false;
    }

    if (formData.new_user_type === 0 && $formWrapper.find('.wpestate_register_form_user_type').length > 0) {
        $messageArea.html(`<div class="login-alert">${control_vars.user_type_warn}</div>`);
        return false;
    }

    return true;
}




/**
 * Handles the registration response from the server.
 * 
 * @param {jQuery} $formWrapper - The form wrapper element.
 * @param {jQuery} $messageArea - The message area element.
 * @param {Object} response - The server response object.
 */
function wpestate_handleRegistrationResponse($formWrapper, $messageArea, response) {
  
    if(response.register === true) {
        $messageArea.html(`<div class="wpestate_success">${response.message}</div>`);
    }else{
        $messageArea.html(`<div class="login-alert">${response.message}</div>`);
    }


    // Clear form fields
    $formWrapper.find('input[type="text"],input[type="email"], input[type="password"]').val('');
    $formWrapper.find('select').val(0);

    if(response.register === true) {
        setTimeout(() => {
            $formWrapper.parent().find('.wpestate_login_form_switch_login').trigger('click');
        }, 1500);
    }
}




/**
 * Set up event listeners for the login functionality.
 * 
 * This function attaches event listeners to login-related elements:
 * - Click event on the login submit button
 * - Keydown event (Enter key) on login form fields
 * 
 * When triggered, these events call the wpestate_login_user function
 * to process the login attempt.
 * 
 * @since 1.0.0
 * 
 * @returns {void}
 */
function wpestate_trigger_login_action() {
    const $document = jQuery(document);
    const ENTER_KEY_CODE = 13;

    // Handle click on login submit button
    $document.on('click', '.wpestate_login_submit_button', function(event) {
        // Call login function, passing the clicked button as a jQuery object
        wpestate_login_user(jQuery(this));
    });

    // Handle Enter key press on login form fields
    $document.on('keydown', '.wpestate_login_form_username, .wpestate_login_form_password', function(event) {
        if (event.keyCode === ENTER_KEY_CODE) {
            event.preventDefault(); // Prevent default Enter key behavior
            
            // Find the submit button within the same form wrapper
            const $submitButton = jQuery(this).closest('.wpestate_login_form_wrapper').find('.wpestate_login_submit_button');
            
            // Call login function, passing the found submit button as a jQuery object
            wpestate_login_user($submitButton);
        }
    });
}


/**
 * Process user login attempt
 * 
 * This function handles the user login process. It collects form data,
 * validates it, sends an AJAX request to the server for authentication,
 * and handles the response.
 *  * @param {jQuery} $submitButton - The jQuery object of the submit button that triggered the login attempt.
 */

function wpestate_login_user($submitButton) {
    "use strict";

    const $formWrapper = $submitButton.closest('.wpestate_login_form_wrapper');
    const $messageArea = $formWrapper.find('.login_register_message_area');
    const ajaxurl = wpestate_login_register_vars.admin_url + 'admin-ajax.php';
    // Display processing message
    $messageArea.html('<div class="login-alert">' + wpestate_login_register_vars.login_loading + '</div>');

    // Collect form data
    const formData = {
        login_user: $formWrapper.find('.wpestate_login_form_username').val(),
        login_pwd: $formWrapper.find('.wpestate_login_form_password').val(),
        security_nonce: $formWrapper.find('.wpestate_social_login_nonce').val(),
        ispop: $formWrapper.find('.loginpop').val() || '0'
    };

    if (jQuery('#loginpop_submit').val() === '3') {
        ispop = 3;
    }

    // Validate form data
    if (!wpestate_validateLoginData($formWrapper, formData)) {
        return;
    }

    // Send AJAX request
    jQuery.ajax({
        type: 'POST',
        dataType: 'json',
        url: ajaxurl,
        data: {
            action: 'wpestate_ajax_login_user',
            ...formData
        },
        success: function(response) {
            wpestate_handleLoginResponse($formWrapper, $messageArea, response);
        },
        error: function(jqXHR, textStatus, errorThrown) {
            console.error('Login error:', jqXHR.responseText, textStatus, errorThrown);
            $messageArea.html('<p>An error occurred during login. Please try again.</p>');
        }
    });
}



/**
 * Validate login form data
 * 
 * @param {jQuery} $formWrapper - The form wrapper jQuery object
 * @param {Object} formData - The form data object
 * @returns {boolean} - True if data is valid, false otherwise
 */
function wpestate_validateLoginData($formWrapper, formData) {
    // Implement validation logic here
    // For example:
    if (!formData.login_user || !formData.login_pwd) {
        $formWrapper.find('.login_register_message_area').html('<div class="login-alert">Please enter both username and password.</div>');
        return false;
    }
    return true;
}

/**
 * Handle login response from server
 * 
 * @param {jQuery} $formWrapper - The form wrapper jQuery object
 * @param {jQuery} $messageArea - The message area jQuery object
 * @param {Object} response - The server response object
 */



function wpestate_handleLoginResponse($formWrapper, $messageArea, response) {
    const extraClass = response.loggedin ? 'wpestate_success' : '';
   

    if(response.loggedin === true) {
        $messageArea.html(`<div class="wpestate_success">${response.message}</div>`);
    }else{
        $messageArea.html(`<div class="login-alert">${response.message}</div>`);
    }

    if (response.loggedin) {
        if (parseInt(response.ispop, 10) === 1 || parseInt(response.ispop, 10) === 3  ) {
            ajaxcalls_vars.userid = response.newuser;
            jQuery('#login-modal_close').trigger('click');
       

            // update menu bar 

            var usericon = '<div class="menu_user_picture" style="background-image: url(' + response.picture + ')"></div>    <a class="navicon-button x"><div class="navicon"></div></a>';
        
           
            jQuery('#user_menu_u').append(usericon).addClass('user_loged');
            jQuery('.submit_action').remove();
            jQuery('.submit_listing').remove();
            jQuery('#agent_property_ajax_nonce').val(response.nonce_contact);
            jQuery('#user_menu_u').after(response.menu.desktop);


            //update mobile menu bar
            jQuery('.login_sidebar').hide();
            jQuery('.mobilemenu-close-user').after(response.menu.mobile);
   
            var mobile_user_icon = '<div class="menu_user_picture" style="background-image: url(' + response.picture + ');"></div>'
            jQuery('.mobile-trigger-user').empty().append(mobile_user_icon);
            // close user drawer if on movbie
            if(wpestate_isMobileOrTablet()){
                jQuery('.mobilemenu-close-user').trigger('click');
            }
         
            wpestate_open_menu();

        }else if (parseInt(response.ispop, 10) === 2) {   
          
            location.reload();

        } else {
            document.location.href = wpestate_login_register_vars.login_redirect;
        }


        jQuery('#user_not_logged_in').hide();
        jQuery('#user_logged_in').show();
    } else {
        $formWrapper.find('.wpestate_login_form_username').val('');
        $formWrapper.find('.wpestate_login_form_password').val('');
    }
}





/**
 * Set up event listeners for forgot password functionality
 * 
 * This function handles forgot password actions including
 * button clicks and Enter key presses on email input fields.
 * 
 * @since 1.0.0
 * 
 * @requires jQuery
 * @requires wpestate_forgot function (assumed to exist)
 */
function wpestate_trigger_forgot_password_action() {
    const $document = jQuery(document);
    const ENTER_KEY_CODE = 13;

    // Handle click on forgot password submit buttons
    $document.on('click', '.wpestate_forgot_password_submit_button', function(event) {
       
        wpestate_forgot( jQuery(this) );
    });

    // Handle Enter key press on forgot password email input fields
    $document.on('keydown', '.wpestate_forgot_form_email', function(event) {
        if (event.keyCode === ENTER_KEY_CODE) {
            event.preventDefault();
            const $submitButton = jQuery(this).closest('.wpestate_forgot_form_wrapper').find('.wpestate_forgot_password_submit_button');
          
            wpestate_forgot($submitButton);
        }
    });
}





/**
 * Process forgot password request
 *
 * @param {jQuery} $submitButton - The jQuery object of the submit button
 */
function wpestate_forgot($submitButton) {
    "use strict";

    const $formWrapper = $submitButton.closest('.wpestate_forgot_form_wrapper');
    const $messageArea = $formWrapper.find('.login_register_message_area');
    const ajaxurl = wpestate_login_register_vars.admin_url + 'admin-ajax.php';

    // Display processing message
    $messageArea.html('<div class="login-alert">' + wpestate_login_register_vars.procesing + '</div>');

    // Collect form data
    const formData = {
        forgot_email: $formWrapper.find('.wpestate_forgot_form_email').val(),
        security_nonce: $formWrapper.find('.wpestate_forgot_form_security').val(),
        postid: $formWrapper.find('#postid').val() || '',
      
    };

    // Validate form data
    if (!wpestate_validateForgotPasswordData($formWrapper, formData)) {
        return;
    }

    // Send AJAX request
    jQuery.ajax({
        type: 'POST',
        dataType: 'json',
        url: ajaxurl,
        data: {
            action: 'wpestate_ajax_forgot_pass_user',
            ...formData
        },
        success: function(response) {
            wpestate_handleForgotPasswordResponse($formWrapper, $messageArea, response);
        },
        error: function(jqXHR, textStatus, errorThrown) {
            console.error('Forgot password error:', textStatus, errorThrown);
            $messageArea.html('<p>An error occurred. Please try again.</p>');
        }
    });
}

/**
 * Validate forgot password form data
 *
 * @param {jQuery} $formWrapper - The form wrapper jQuery object
 * @param {Object} formData - The form data object
 * @returns {boolean} - True if data is valid, false otherwise
 */
function wpestate_validateForgotPasswordData($formWrapper, formData) {

    if (!formData.forgot_email) {
        $formWrapper.find('.login_register_message_area').html(`<div class="login-alert">${wpestate_login_register_vars.forgot_warning}</div>`);
        return false;
    }
    return true;
}

/**
 * Handle forgot password response
 *
 * @param {jQuery} $formWrapper - The form wrapper jQuery object
 * @param {jQuery} $messageArea - The message area jQuery object
 * @param {Object} response - The response from the server
 */
function wpestate_handleForgotPasswordResponse($formWrapper, $messageArea, response) {
    const extraClass = response.reset === true ? 'wpestate_success' : '';
    $formWrapper.find('.wpestate_forgot_form_email').val('');
    $messageArea.html(`<div class="login-alert ${extraClass}">${response.message}</div>`);
}
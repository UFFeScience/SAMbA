/**
 *
 * @param {String} parameter
 * @param {String} staticURL
 * @param {bollean} decode
 * @returns {String|Boolean}
 */
function getUrlParameters(parameter, staticURL, decode) {
    /*
     Function: getUrlParameters
     Description: Get the value of URL parameters either from
     current URL or static URL
     Author: Tirumal
     URL: www.code-tricks.com
     */
    try {
        var currLocation = (staticURL.length) ? staticURL : window.location.search,
            parArr = currLocation.split("?")[1].split("&"),
            returnBool = true;

        for (var i = 0; i < parArr.length; i++) {
            parr = parArr[i].split("=");
            if (parr[0] == parameter) {
                if (returnBool)
                    returnBool = true;
                return (decode) ? decodeURIComponent(parr[1]) : parr[1];
            } else {
                returnBool = false;
            }
        }

        if (!returnBool)
            return false;
    } catch (err) {
        return null;
    }
}
import BaseError from "@utils/errors/baseError";
import { ErrorCodes } from "@utils/errors/errorCodes";

class AuthorizationError extends BaseError {
    constructor(message = "Invalid or missing admin token") {
        super(ErrorCodes.UNAUTHORIZED, message);
    }
}
export default AuthorizationError;

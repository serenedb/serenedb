import BaseError from "@utils/errors/baseError";
import { ErrorCodes } from "@utils/errors/errorCodes";

/** The backend booted without a config and none was pushed yet (409). */
class NotConfiguredError extends BaseError {
    constructor() {
        super(ErrorCodes.CONFLICT, "Backend not configured yet");
    }
}
export default NotConfiguredError;

import BaseError from "@utils/errors/baseError";
import { ErrorCodes } from "@utils/errors/errorCodes";

class ServiceUnavailableError extends BaseError {
    constructor(message: string) {
        super(ErrorCodes.SERVICE_UNAVAILABLE, message);
    }
}
export default ServiceUnavailableError;

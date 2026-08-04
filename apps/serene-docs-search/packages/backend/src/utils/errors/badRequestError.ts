import BaseError from "@utils/errors/baseError";
import { ErrorCodes } from "@utils/errors/errorCodes";

class BadRequestError extends BaseError {
    constructor(message: string) {
        super(ErrorCodes.BAD_REQUEST, message);
    }
}
export default BadRequestError;

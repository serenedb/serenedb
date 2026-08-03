import BaseError from "@utils/errors/baseError";
import { ValidationError } from "class-validator";
import { ErrorCodes } from "@utils/errors/errorCodes";

class CustomValidationError extends BaseError {
    propertyNames: string;

    constructor(errors: ValidationError[]) {
        const propertiesNames = errors
            .map((err) => {
                return err.property;
            })
            .join(" ");

        super(
            ErrorCodes.BAD_REQUEST,
            `Field${errors.length === 1 ? "" : "s"} '${propertiesNames}' filled incorrectly`,
        );

        this.propertyNames = propertiesNames;
    }
}
export default CustomValidationError;

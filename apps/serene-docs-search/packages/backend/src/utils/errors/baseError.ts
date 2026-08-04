class BaseError extends Error {
    statusCode: number;

    constructor(statusCode: number, message: string) {
        super(message);

        Object.setPrototypeOf(this, new.target.prototype);
        this.name = new.target.name;
        this.statusCode = statusCode;
        Error.captureStackTrace(this, new.target);
    }
}
export default BaseError;

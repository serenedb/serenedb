export type SuccessResponse<T> = {
    success: true;
    message: string;
    data: T;
};

export interface ErrorResponse {
    success: false;
    code: number;
    message: string;
}

export const responseSuccess = <T>(
    data: T,
    message?: string,
): SuccessResponse<T> => ({
    success: true,
    message: message ?? "Success",
    data,
});

export const responseError = (
    code: number,
    message?: string,
): ErrorResponse => ({
    success: false,
    code,
    message: message ?? "Error",
});

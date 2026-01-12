import { WithEntities, WithFeatures, WithPages } from "../providers";

export const LoaderLayout = ({ children }: { children: React.ReactNode }) => {
    return (
        <WithEntities>
            <WithFeatures>
                <WithPages>{children}</WithPages>
            </WithFeatures>
        </WithEntities>
    );
};

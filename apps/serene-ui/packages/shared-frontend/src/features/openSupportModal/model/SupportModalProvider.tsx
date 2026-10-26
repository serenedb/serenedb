import { createContext, useContext, useState } from "react";
import { SupportModal } from "../ui";

interface SupportModalContextType {
    open: boolean;
    setOpen: React.Dispatch<React.SetStateAction<boolean>>;
}

const SupportModalContext = createContext<SupportModalContextType | undefined>(
    undefined,
);

export const SupportModalProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const [open, setOpen] = useState(false);
    return (
        <SupportModalContext.Provider
            value={{
                open,
                setOpen,
            }}>
            {children}
            <SupportModal />
        </SupportModalContext.Provider>
    );
};

export const useSupportModal = () => {
    const context = useContext(SupportModalContext);
    if (!context) {
        throw new Error(
            "useSupportModal must be used within a SupportModalProvider",
        );
    }
    return context;
};

import React from "react";
import { Button, Logo } from "../components/primitives";
import { useSpinner } from "../hooks/useSpinner";
import { Deploy } from "./Deploy";
import { Indexing } from "./Indexing";
import { StepContent } from "./steps/StepContent";
import { StepSearch } from "./steps/StepSearch";
import { StepSource } from "./steps/StepSource";
import { StepSync } from "./steps/StepSync";
import type { SetupFlow } from "./useSetupFlow";
import { stepValid } from "./validation";

export interface SetupScreenProps {
    flow: SetupFlow;
    onClose: () => void;
    toast: (msg: string) => void;
}

/** Modal chrome (header / rail / footer) for the wizard, deploy and indexing phases. */
export function SetupScreen({ flow, onClose, toast }: SetupScreenProps): React.ReactElement | null {
    const { phase, step, config, update, goStep, progress } = flow;
    const spin = useSpinner(phase === "indexing" && progress?.state !== "done");

    if (phase === "wizard") {
        return (
            <>
                <div className="sds-header">
                    <Logo className="sds-header-logo" />
                    <div className="sds-header-title">
                        SereneDocsSearch — First-run setup
                    </div>
                    <div className="sds-header-spacer" />
                    <div className="sds-header-step">
                        <b>0{step}</b> / 04
                    </div>
                    <button type="button" className="sds-x" onClick={onClose}>
                        ×
                    </button>
                </div>
                <div className="sds-rail">
                    {[1, 2, 3, 4].map((i) => (
                        <span key={i} className={step >= i ? "on" : ""} />
                    ))}
                </div>
                <div className="sds-body">
                    {step === 1 && <StepSource config={config} update={update} />}
                    {step === 2 && <StepContent config={config} update={update} />}
                    {step === 3 && <StepSearch config={config} update={update} />}
                    {step === 4 && <StepSync config={config} update={update} />}
                </div>
                <div className="sds-footer">
                    <div>
                        {step > 1 && (
                            <Button variant="ghost" onClick={() => goStep(step - 1)}>
                                ← Back
                            </Button>
                        )}
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                        <div className="sds-footer-note">
                            config → serene-search.config.json
                        </div>
                        <Button
                            disabled={!stepValid(config, step)}
                            onClick={() => {
                                if (step < 4) goStep(step + 1);
                                else {
                                    flow.setConn({ kind: "idle" });
                                    flow.setPhase("deploy");
                                }
                            }}
                        >
                            {step === 4 ? "Generate deploy files →" : "Continue →"}
                        </Button>
                    </div>
                </div>
            </>
        );
    }

    if (phase === "deploy") {
        return (
            <>
                <div className="sds-header">
                    <Logo className="sds-header-logo" />
                    <div className="sds-header-title">SereneDocsSearch — Deploy</div>
                    <div className="sds-header-spacer" />
                    <button type="button" className="sds-x" onClick={onClose}>
                        ×
                    </button>
                </div>
                <div className="sds-body">
                    <Deploy
                        config={config}
                        token={flow.token}
                        backendUrl={flow.backendUrl}
                        setBackendUrl={flow.setBackendUrl}
                        tokenInput={flow.tokenInput}
                        setTokenInput={flow.setTokenInput}
                        conn={flow.conn}
                        setConn={flow.setConn}
                        toast={toast}
                    />
                </div>
                <div className="sds-footer">
                    <Button variant="ghost" onClick={() => flow.setPhase("wizard")}>
                        ← Back
                    </Button>
                    <Button
                        disabled={flow.conn.kind !== "ok"}
                        onClick={() => void flow.startIndexing()}
                    >
                        Pull files &amp; build index →
                    </Button>
                </div>
            </>
        );
    }

    if (phase === "indexing") {
        return (
            <>
                <div className="sds-header">
                    <Logo className="sds-header-logo" />
                    <div className="sds-header-title">SereneDocsSearch — Indexing</div>
                    <div className="sds-header-spacer" />
                    <div className="sds-header-status">
                        {progress?.state === "done"
                            ? "✓ done"
                            : progress?.state === "error"
                              ? "✕ failed"
                              : `${spin} running`}
                    </div>
                </div>
                <div className="sds-body">
                    <Indexing progress={progress} config={config} />
                </div>
                <div className="sds-footer" style={{ justifyContent: "flex-end" }}>
                    {progress?.state === "error" ? (
                        <Button variant="secondary" onClick={() => flow.setPhase("deploy")}>
                            ← Back to deploy
                        </Button>
                    ) : (
                        <Button
                            disabled={progress?.state !== "done"}
                            onClick={flow.finishSetup}
                        >
                            Open search →
                        </Button>
                    )}
                </div>
            </>
        );
    }

    return null;
}

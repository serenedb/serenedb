import { oc } from "@orpc/contract";
import { eventIterator } from "@orpc/server";
import { ExecuteQueryInput, SubscribeQueryExecutionInput } from "../inputs";
import { ExecuteQueryOutput, SubscribeQueryExecutionOutput } from "../outputs";

export const executeQueryContract = oc
    .input(ExecuteQueryInput)
    .output(ExecuteQueryOutput);

export const subscribeQueryExecutionContract = oc
    .input(SubscribeQueryExecutionInput)
    .output(eventIterator(SubscribeQueryExecutionOutput));

export const queryExecutionContracts = {
    execute: executeQueryContract,
    subscribe: subscribeQueryExecutionContract,
};

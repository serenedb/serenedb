export const apiMap = {
    connections: {
        getMy: "/connections",
        add: "/connections/add",
        update: "/connections/update",
        delete: "/connections/delete",
    },
    query: {
        execute: "/query",
        checkJob: "/query/check-job",
    },
    savedQueries: {
        getMy: "/saved-queries",
        add: "/saved-queries/add",
        update: "/saved-queries/update",
        delete: "/saved-queries/delete",
    },
    dashboards: {
        getMy: "/dashboards",
        favorites: "/dashboards/favorites",
        add: "/dashboards/add",
        update: "/dashboards/update",
        delete: "/dashboards/delete",
    },
    queryHistory: {
        getMy: "/query-history",
        add: "/query-history/add",
        update: "/query-history/update",
        delete: "/query-history/delete",
    },
    github: {
        startAuthorization: "/github/start-authorization",
        verifyAuthorization: "/github/verify-authorization",
        authorized: "/github/authorized",
        leaveStar: "/github/leave-star",
        unauthorize: "/github/unauthorize",
        createIssue: "/github/create-issue",
    },
};

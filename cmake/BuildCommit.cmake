set(commit "unknown")
if(EXISTS "${GIT_EXECUTABLE}")
    execute_process(
        COMMAND
            ${CMAKE_COMMAND} -E env GIT_OPTIONAL_LOCKS=0 ${GIT_EXECUTABLE} -C
            ${SOURCE_DIR} rev-parse HEAD
        RESULT_VARIABLE failed
        OUTPUT_VARIABLE head
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    if(NOT failed)
        set(commit "${head}")
        execute_process(
            COMMAND
                ${CMAKE_COMMAND} -E env GIT_OPTIONAL_LOCKS=0 ${GIT_EXECUTABLE}
                -C ${SOURCE_DIR} diff --quiet --ignore-submodules=dirty HEAD --
            RESULT_VARIABLE dirty
            OUTPUT_QUIET
            ERROR_QUIET
        )
        if(NOT dirty EQUAL 0)
            string(APPEND commit "-dirty")
        endif()
    endif()
endif()
file(
    CONFIGURE
    OUTPUT ${OUTPUT}
    CONTENT "#define SERENEDB_COMMIT \"@commit@\"\n"
    @ONLY
)

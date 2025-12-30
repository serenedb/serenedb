# Getting started

## Fork the repository

<p align="center">
  <img src="https://github.com/user-attachments/assets/82327bc5-331e-49af-8b5c-717f563b67d4" width="800" style="border-radius: 8px;">
</p>

## Clone the repository

```bash
# specify github username
GITHUB_USERNAME="your_github_username_here"
git clone git@github.com:${GITHUB_USERNAME}/serenedb.git
cd serenedb
git remote add upstream git@github.com:serenedb/serenedb.git
git submodule update --init --recursive --jobs=$(nproc)
```

## Build Prerequisites

1) Compiler: clang-21 / clang++-21
2) Build system: Ninja
3) CMake >= 3.26

## Build

Use cmake with preset 'lldb' to build it in debug. Additional build presets are defined in `CMakePresets.json` file.

```
# from the root of the repository
cmake --preset lldb -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21
cd build/
ninja
```

## Launch

Now you can launch serened binary:

```
# from the root of the repository
./build/bin/serened ./build_dir --server.endpoint='pgsql+tcp://0.0.0.0:7777'
```

It's possible to connect it via psql: `psql -h localhost -p 7777 -U postgres`


## Test

Commands in this section are supposed to be executed from the root of the repository.

### Sql-logic tests

The tests require a running SereneDB instance and pre-downloaded cargo.

Launch all the tests:
```bash
./tests/sqllogic/run.sh --single-port 7777 --debug true
```

Also it's possible to specify a filter:

```bash
./tests/sqllogic/run.sh --single-port 7777 --test 'tests/sqllogic/any/pg/simple/*.test' --debug true
```

### C++ unit tests

```bash
./build/bin/iresearch-tests "--gtest_filter=*PhraseFilterTestCase*"
```

# Convenient work in VSCode

## Importing a profile

We have a VScode profile which has already all the extensions which are needed (for instance for code navigation). Here is a config of this:
```json
{
  "name": "SereneDB C++ template",
  "settings": "{\"settings\":\"{\\n    \\\"window.titleBarStyle\\\": \\\"custom\\\",\\n    \\\"files.trimFinalNewlines\\\": true,\\n    \\\"files.insertFinalNewline\\\": true,\\n    \\\"workbench.settings.applyToAllProfiles\\\": [\\n        \\\"files.insertFinalNewline\\\",\\n        \\\"files.trimFinalNewlines\\\",\\n        \\\"editor.inlayHints.enabled\\\",\\n        \\\"remote.autoForwardPorts\\\",\\n        \\\"files.autoSave\\\",\\n        \\\"editor.minimap.enabled\\\"\\n    ],\\n    \\\"editor.inlayHints.enabled\\\": \\\"off\\\",\\n    \\\"remote.autoForwardPorts\\\": false,\\n    \\\"files.autoSave\\\": \\\"afterDelay\\\",\\n    \\\"settingsSync.ignoredSettings\\\": [\\n        \\\"-clangd.path\\\"\\n    ],\\n    \\\"clangd.arguments\\\": [\\n        \\\"--compile-commands-dir=${workspaceFolder}/build\\\",\\n        \\\"--function-arg-placeholders=0\\\",\\n        \\\"--header-insertion=never\\\"\\n    ],\\n    \\\"window.newWindowProfile\\\": \\\"Default\\\",\\n    \\\"editor.minimap.enabled\\\": false,\\n    \\\"compilerexplorer.compilationDirectory\\\": \\\"${workspaceFolder}/build_rel\\\",\\n    \\\"editor.defaultFormatter\\\": \\\"llvm-vs-code-extensions.vscode-clangd\\\",\\n    \\\"extensions.ignoreRecommendations\\\": true,\\n    \\\"clangd.checkUpdates\\\": true,\\n    \\\"editor.tabSize\\\": 2,\\n    \\\"workbench.remoteIndicator.showExtensionRecommendations\\\": false\\n}\\n\"}",
  "extensions": "[{\"identifier\":{\"id\":\"github.remotehub\",\"uuid\":\"fc7d7e85-2e58-4c1c-97a3-2172ed9a77cd\"},\"displayName\":\"GitHub Repositories\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"harikrishnan94.cxx-compiler-explorer\",\"uuid\":\"68ef4789-1f8c-4d80-b929-cfb718979aa2\"},\"displayName\":\"C/C++ Compiler explorer\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"llvm-vs-code-extensions.vscode-clangd\",\"uuid\":\"103154cb-b81d-4e1b-8281-c5f4fa563d37\"},\"displayName\":\"clangd\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.remote-containers\",\"uuid\":\"93ce222b-5f6f-49b7-9ab1-a0463c6238df\"},\"displayName\":\"Dev Containers\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.remote-ssh\",\"uuid\":\"607fd052-be03-4363-b657-2bd62b83d28a\"},\"displayName\":\"Remote - SSH\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.remote-ssh-edit\",\"uuid\":\"bfeaf631-bcff-4908-93ed-fda4ef9a0c5c\"},\"displayName\":\"Remote - SSH: Editing Configuration Files\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.vscode-remote-extensionpack\",\"uuid\":\"23d72dfc-8dd1-4e30-926e-8783b4378f13\"},\"displayName\":\"Remote Development\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode.remote-explorer\",\"uuid\":\"11858313-52cc-4e57-b3e4-d7b65281e34b\"},\"displayName\":\"Remote Explorer\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode.remote-repositories\",\"uuid\":\"cf5142f0-3701-4992-980c-9895a750addf\"},\"displayName\":\"Remote Repositories\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode.remote-server\",\"uuid\":\"105c0b3c-07a9-4156-a4fc-4141040eb07e\"},\"displayName\":\"Remote - Tunnels\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"vadimcn.vscode-lldb\",\"uuid\":\"bee31e34-a44b-4a76-9ec2-e9fd1439a0f6\"},\"displayName\":\"CodeLLDB\",\"applicationScoped\":false}]"
}
```

There's a gif guide with an action list below:

<p align="center">
  <img src="https://github.com/user-attachments/assets/02f2e2f9-b9d6-407d-832a-2517254dee98" width="800" style="border-radius: 8px;">
</p>

0) Open a folder with SereneDB.
1) Just create a `serenedb-cpp.code-profile` file in the root and paste the profile-config there.
2) Open a VSCode command palette via default combination : ctrl + shift + p / cmd + shift + p for macOS.
3) Write in the palette `Open Profiles` and choose an `Preferences: Open Profiles (UI)` option.
4) In the UI of the profiles click on the down arrow which is located left to the `New Profile` button.
5) Choose import profile and specify a path to the `serenedb-cpp.code-profile`.
6) Create the profile and switch to it.
7) If a message appears offering to download the clangd server, accept it.

Now you can use C++ code navigation by ctrl + click (cmd + click for macOS)!

## Debug

Also VSCode provides a convenient way to debug code. There're some steps to make it work:

1) Create a `.vscode/launch.json` file.
2) Paste there a config:
```json
{
  "configurations": [
    {
      "type": "lldb",
      "request": "attach",
      "name": "attach-to-serened",
      "program": "${workspaceFolder}/build/bin/serened",
    },
    {
      "type": "lldb",
      "request": "launch",
      "name": "iresearch",
      "program": "${workspaceFolder}/build/bin/iresearch-tests",
      "args": [
        "--gtest_filter=*PhraseFilterTestCase*"
      ],
      "cwd": "${workspaceFolder}"
    }
  ]
}
```
3) Click `Run And Debug` section on the left sidebar or use a combination of keys: shift + ctrl + d / shift + cmd + d for macOS.

This will add two types of actions - `attach` for attaching to the running SereneDB and `iresearch` to launch the unit-test and attach to it. You can click the down arrow to the right of the green triangle and select the desired option from the drop-down list.

There's an example of the creation of these actions and attaching to the SereneDB via debugger:

<p align="center">
  <img src="https://github.com/user-attachments/assets/fa246b5d-ebea-4598-8705-c252fbff5a0d" width="800" style="border-radius: 8px;">
</p>

# Thank you for your contribution <3

<p align="center">
  <img src="https://github.com/user-attachments/assets/86dedb73-478f-4344-9dcb-320200435b99" width="300" style="border-radius: 8px;">
</p>

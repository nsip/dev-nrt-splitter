# dev-nrt-splitter

NRT-Splitter is a command-line utility that does post-processes for csv reports. It walks through a designated folder to deal with each valid csv files, and ignores any other file type.

## basic usage

Download a pre-built binary distribution for your platform (Windows, Linux, Mac), from the releases area of this repository:

<https://github.com/nsip/dev-nrt-splitter/releases/latest>

Unpack the downloaded zip file, and you should see a folder structure like this:

```txt
/NRT-Splitter-Linux-v0_0_1
    report_splitter(.exe)
    config.toml
    /data
        └──system_reports.zip
```

In the folder:

- the report_splitter executable (report_splitter.exe if on windows)
- a configuration file (config.toml)
- a subfolder called /data which has a sample package file (system_reports.zip)

report_splitter ignores any command-line parameters or flags except its designated configuration file path.

If running report_splitter without designating configuration file path, default `config.toml` under same directory would apply to executable.

### configuration

```toml
# This is a sample configuration.

InFolder = "./in/"      # (string), in which folder splitter processes report csv files.
WalkSubFolders = false  # (bool), if true, splitter process every file including the file in sub-folders; otherwise, ignores sub-folder files.

[Trim]
Enabled = true                            # (bool), turn on/off Trim function.
Columns = ["School", "Yrlevel", "Domain"] # (string array), which columns to be removed from original csv file.
OutFolder = "./out/"                      # (string), in which folder trimmed csv files should be output.

[Splitting]
Enabled = true                            # (bool), turn on/off Splitting function.
OutFolder = "./out/"                      # (string), in which folder split results should be output.
Schema = ["School", "Yrlevel", "Domain"]  # (string array), header sequence for splitting. Each header creates its split category folder. 
```

### play with sample

1. Under `/NRT-Splitter-Linux-v0_0_1`, unpack sample package, `unzip ./data/system_reports.zip -d ./in/`.
2. Modify `config.toml`, set `InFolder` value to `"./in/"`.
3. Make sure `config.toml` is in the same directory of report_splitter.
4. Run `./report_splitter(.exe)`.
5. Results should be in `./out/` (as you set in configuration) folder after running `./report_splitter(.exe)`.

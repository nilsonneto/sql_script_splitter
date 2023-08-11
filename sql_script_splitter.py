import os
import re
import sys
import yaml

YAML_FOLDER_PATH = "sql_script_splitter.yaml"


class SmallScript:
    """
    Class to hold information of each individual script of a larger script, that contains several CTEs
    """

    def __init__(self, old_script: str, base_script: str, final_script: str) -> None:
        # Initial cleanup so metadata can be more easily extracted
        clean_script = self.clean_query(old_script)

        # From first line, decide if it is a intermediate table or a single/last table.
        first_line, _, other_lines = clean_script.partition("\n")

        # Cleaning up the first line, removing the `,` marker, empty whitespace and splitting it
        first_words = first_line.strip().strip(",").lower().split(" ")
        # Removing empty words (`None`s and `''`s) from list
        first_words = [word for word in first_words if word]
        first_word = first_words[0]
        second_word = first_words[1] if len(first_words) > 1 else None

        if first_word in ["{{", "select"]:
            # If it has {{ it is a script with DBT config
            # or if it begins with select is either:
            # * Single script
            # * Final step
            content = clean_script
            old_name = base_script
            new_name = final_script
            is_intermediate = False
        else:
            # All other cases
            # Table name is extracted from first words of script
            # And content comes from other_lines, so it has to be cleaned up again
            content = self.clean_query(other_lines)
            old_name = first_word if first_word != "with" else second_word
            new_name = f"{final_script}_{old_name}"
            is_intermediate = True

        self.content = content
        self.old_name = old_name
        self.new_name = new_name
        self.is_intermediate = is_intermediate
        self.new_reference = f' {{{{ ref("{new_name}") }}}}'
        self.re_expr = re.compile(rf"(?<=(from|join))\s{{1,}}{old_name}")

    def __str__(self):
        return f'{self.old_name} -> {self.new_name} ({"CTE" if self.is_intermediate else "last"}):\n{self.content}'

    @staticmethod
    def clean_query(query: str) -> str:
        """
        Cleans up the content property, update its internal value and return it
        """
        new_query = query

        # Cleaning start of query
        while True:
            split_script = new_query.split("\n", 1)
            first = split_script[0].strip()
            # Remove blank lines and commentaries from parsing
            if first == "" or first.startswith("--") or first.startswith("//"):
                new_query = split_script[1]
            else:
                break

        # Cleaning end of query
        while True:
            split_script = new_query.rsplit("\n", 1)
            last = split_script[1].strip()
            # Remove blank lines and commentaries from parsing
            if last == "" or last.startswith("--") or last.startswith("//"):
                new_query = split_script[0]
            else:
                break

        # When query is wrapped around parenthesis, remove them
        if new_query.strip().startswith("(") and new_query.strip().endswith(")"):
            new_query = new_query.strip()[1:-1]
            if new_query[0] == "\n":
                new_query = new_query[1:]

        # Cleaning up trailing whitespaces
        clean_list = [line.rstrip() for line in new_query.splitlines()]
        new_query = "\n".join(clean_list)

        # Always good to have an empty line at the end of the query
        new_query += "\n"

        return new_query


class SplitParameters:
    """
    Class that holds information used on the main split function.
    Below are the variables and their purposes:

    scripts_base_path (str): Base path to se any script
    initial_script (str): Script to split
    final_script (str): Name of the final script
    drop_intermediate (bool): Add drop statements to final script
    """

    def __init__(
        self,
        scripts_base_path: str,
        initial_script: str,
        final_script: str,
        drop_intermediate: bool,
    ) -> None:
        self.scripts_base_path = scripts_base_path
        self.initial_script = initial_script
        self.final_script = final_script
        self.drop_intermediate = drop_intermediate


def get_individual_scripts_and_dbt_config(base_path: str, base_script: str):
    """
    Reads file located at `{base_path}\{base_script}.sql`, and splits into chunks.
    Also, extracts the DBT config, which is expected to be at the beginning of the file,
    before the `with` keyword.
    """
    script_base_path = os.path.join(base_path, f"{base_script}.sql")
    with open(script_base_path) as vbase:
        all = vbase.read()

    # Splits based on the `, table as`, with some room for optional comments and spacing
    # That line is kept, some line-breaks are disposed and everything inside the parenthesis
    # Is kept, but in a separate object
    slices = re.split(r"\n(,.* as.*)\n?(?=\()", all)

    # First slice works differently
    # It contains DBT Config and first CTE, so they are separated based on the `with` keyword
    # And first slice is removed from slice list
    dbt_config, first_cte = re.split(r"\nwith ", slices[0], 1)
    slices = slices[1:]

    # Since first line and query were separated by the split function
    # They will be joined back, two by two
    # And script list is formed of the first CTE and the joined back CTEs
    scripts = [first_cte]
    scripts += [sc1 + "\n" + sc2 for sc1, sc2 in zip(slices[::2], slices[1::2])]

    # Last item is actually the last CTE + the final table query
    # So it is removed, and broken apart with the logic that
    # -> a line starting by closing parenthesis `)`,
    # -> optionally followed by comments and whitespace,
    # -> followed by a `select` (case-insensitive),
    # is the place to split both the last CTE from the final table
    final_slice = scripts.pop()
    last_scripts = re.split(r"(?i)(?<=\))((?:(?:[ ]*--.*)?\n)+select)", final_slice)
    last_cte = last_scripts[0]
    final_table = last_scripts[1] + last_scripts[2]

    # Finally, they are added back into the script list
    scripts += [last_cte, final_table]

    return scripts, dbt_config


def dbt_cfg_enable_table(dbt_config: str) -> str:
    """
    Modifies the "enabled" parameter of the dbt_config to `true`, if it has been set to `false`.
    If parameter is not present it will follow lower priority values (e.g.: cmd value, dbt_project, etc.) or the `true`
    value, which is the default value.
    """
    enable_r = re.compile(r"enabled[ ]{0,}?=[ ]{0,}?false")
    if enable_r.search(dbt_config):
        dbt_config = enable_r.sub("enabled = true", dbt_config)
    return dbt_config


def dbt_cfg_add_drop(dbt_config: str, scripts: list[SmallScript]) -> str:
    """
    Adds drop table statements to the `post-hook` section of the DBT config
    """
    post_r = re.compile(r"post_hook\s*=\s*\[(\s*)")
    post_search = post_r.search(dbt_config)

    if not post_search:
        # TODO: Add support for empty DBT config
        raise Exception(
            "post_hook not found, please add it so drop statements can be added."
        )

    post_groups = post_search.groups()
    post_spacing = post_groups[0] if len(post_groups) >= 1 else "\n    "

    post_string = f"post_hook = [{post_spacing}"
    for scr in scripts:
        if scr.is_intermediate:
            post_string += f"'drop table {scr.new_reference}',{post_spacing}"

    if post_search:
        dbt_config = post_r.sub(post_string, dbt_config)

    return dbt_config


def find_model_path(scripts_base_path: str, model_name: str) -> str:
    """
    Searches for a SQL file with the `model_name`
    in the `scripts_base_path` directory and all others in it, recursevely.

    Returns the file's full path or raises an Exception if file is not found.
    """
    desired_model = f"{model_name}.sql"
    for root, _, files in os.walk(scripts_base_path):
        if (desired_model) in files:
            return root

    raise Exception("Model not found")


def delete_stale_scripts(path: str, initial: str, final: str) -> None:
    """
    Deletes all sql files that are related to the final script, except the initial one.
    Uses yaml information, so, if the final script name changes,
    this function might not be able to remove some files.
    """
    for obj in os.listdir(path):
        if obj == f"{initial}.sql":
            continue
        elif obj.startswith(final) and obj.endswith(".sql"):
            os.remove(os.path.join(path, obj))


def create_new_script_files(
    scripts: list[SmallScript],
    scripts_path: str,
    drop_intermediate: bool,
    dbt_config: str,
) -> None:
    """
    Creates files for list of `SmallScript`s at `scripts_path`.
    If `drop_intermediate` is True, post-hook statements will be added to the DBT config of the final table.
    """
    # Enable final model
    dbt_config = dbt_cfg_enable_table(dbt_config)

    # Write all CTEs and final model
    for scr in scripts:
        # Replace tables with ref macro
        for ref_script in scripts:
            scr.content = ref_script.re_expr.sub(
                ref_script.new_reference,
                scr.content,
            )

        # Add drop statements in last table, if enabled
        if drop_intermediate:
            dbt_config = dbt_cfg_add_drop(dbt_config, scripts)

        # Add DBT config to final tables
        if not scr.is_intermediate:
            scr.content = dbt_config + "\n" + scr.content

        file_path = os.path.join(scripts_path, f"{scr.new_name}.sql")
        with open(file_path, "w") as f:
            f.write(scr.content)


def split_script_into_files(split_params: SplitParameters) -> None:
    """
    Splits script from `scripts_path` folder, from file `initial_script`.sql, into smaller files.
    Final script will be named `final_script`.sql.
    If `drop_intermediate` is True, post-hook statements will be added to the DBT config of the final table.
    """
    scripts_base_path = split_params.scripts_base_path
    initial_script = split_params.initial_script
    final_script = split_params.final_script
    drop_intermediate = split_params.drop_intermediate

    scripts_path = find_model_path(scripts_base_path, initial_script)
    scripts, dbt_config = get_individual_scripts_and_dbt_config(
        scripts_path, initial_script
    )

    # Converts all scripts into SmallScript objects
    small_scripts = [SmallScript(scr, initial_script, final_script) for scr in scripts]
    # DEBUG: print([str(scr) for scr in small_scripts])

    # Remove stale and recreate all script files
    delete_stale_scripts(scripts_path, initial_script, final_script)
    create_new_script_files(small_scripts, scripts_path, drop_intermediate, dbt_config)


def get_parameters_list_from_yaml() -> list[SplitParameters]:
    """
    Reads `sql_script_splitter.yaml` file to extract parameters used in the main function.
    """
    if len(sys.argv) >= 3:
        yaml_arg_path = sys.argv[2]
    else:
        yaml_arg_path = None

    # File of argument has higher priority than file on folder
    yaml_path = yaml_arg_path if yaml_arg_path else YAML_FOLDER_PATH

    # Checks if at least one of these are true:
    # * File is on the same folder as script
    # * Was it passed as argument and it exists
    if not os.path.exists(yaml_path):
        raise Exception(f"YAML config file ({yaml_path}) does not exist.")

    yaml_abs_path = os.path.abspath(yaml_path)
    print(f"YAML path being loaded: {yaml_abs_path}")
    with open(yaml_abs_path, "r") as yaml_stream:
        yaml_obj = yaml.safe_load(yaml_stream)
        print(yaml_obj)
    yaml_root = os.path.dirname(yaml_abs_path)

    models_to_split: dict = yaml_obj.get("models_to_split", None)

    parameters_list: list[SplitParameters] = []
    if models_to_split:
        for model in models_to_split:
            scripts_base_path: str = model.get("scripts_base_path", "")
            is_relative = not os.path.isabs(scripts_base_path)
            if is_relative:
                scripts_base_path = os.path.join(yaml_root, scripts_base_path)

            parameters = SplitParameters(
                scripts_base_path,
                model.get("initial_script", None),
                model.get("final_script", None),
                model.get("drop_intermediate", None),
            )
            parameters_list.append(parameters)

    return parameters_list


def get_parameters_from_argv() -> SplitParameters:
    """
    Reads `sys.argv` and extract all parameters to be used in the main function.
    """

    if len(sys.argv) < 6:
        raise Exception("Invalid number of arguments.")

    # Script name will be sys.argv[0]
    # Type of parameters will be sys.argv[1]
    parameters = SplitParameters(
        scripts_base_path=sys.argv[2],
        initial_script=sys.argv[3],
        final_script=sys.argv[4],
        drop_intermediate=sys.argv[5].lower() == "true",
    )

    return parameters


if __name__ == "__main__":
    type_of_parameters = sys.argv[1]
    if type_of_parameters == "yaml":
        yaml_params = get_parameters_list_from_yaml()
        for params in yaml_params:
            split_script_into_files(params)
    elif type_of_parameters == "cmd":
        params = get_parameters_from_argv()
        split_script_into_files(params)
    else:
        raise Exception("Unknown type of parameters")

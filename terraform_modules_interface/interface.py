import sys

from gitops.better_exchook import install
from gitops_utils.utils import (
    Utils,
    is_nothing,
    get_available_methods,
    get_process_output,
    FilePath,
)
from filesystem_broker.broker import Broker
from terraform_modules_interface.terraform_module_resources import (
    TerraformModuleResources,
)


class Interface(Utils):
    def __init__(
        self,
        module_resources: List[TerraformModuleResources],
        modules_dir: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.module_resources = module_resources

        if is_nothing(modules_dir):
            self.modules_dir = defaults.TERRAFORM_MODULES_DIR
        else:
            self.modules_dir = modules_dir

        self.broker = Broker(**kwargs)

    def build():
        self.logger.info(
            f"Checking for existing Terraform module directories in {self.modules_dir}"
        )
        existing_tf_library_modules = self.broker.scan_dir(
            files_path=self.modules_dir,
            decode=False,
            allowed_extensions=[".library-module"],
            reject_dotfiles=False,
            paths_only=True,
            exit_on_completion=False,
        )

        self.logged_statement(
            "Existing Terraform modules", json_data=existing_tf_library_modules
        )

        new_tf_library_modules = set()

        def update_module_dir(mp: FilePath):
            local_module_path = self.local_path(mp)
            local_module_parent = local_module_path.parent

            flag_file_path = local_module_parent.joinpath(".library-module")

            self.update_file(
                file_path=flag_file_path,
                file_data="# Terraform Module",
            )

            new_tf_library_modules.add(flag_file_path)

            stdout, stderr = get_process_output(
                f"terraform-docs markdown table {local_module_parent}"
            )

            if stdout is None:
                raise RuntimeError(f"Failed to generate Terraform docs: {stderr}")

            self.update_file(
                file_path=local_module_parent.joinpath("README.md"), file_data=stdout
            )

        for terraform_module_resources in self.module_resources:
            if terraform_module_resources.generation_forbidden:
                self.logger.warning(
                    f"f[{terraform_module_resources.module_name}] Generation forbidden, skipping"
                )
                continue

            for (
                target_module_path,
                target_module_json,
            ) in terraform_module_resources.get_modules_to_copy_variables_to():
                self.logger.info(
                    f"[{terraform_module_resources.module_name}] Saving Terraform module variables"
                    f" to {target_module_path}"
                )

                self.update_file(
                    file_path=target_module_path,
                    file_data=target_module_json,
                    encode_with_json=True,
                )

                update_module_dir(target_module_path)

            for (
                foreach_module_path,
                foreach_module_json,
            ) in terraform_module_resources.get_foreach():
                self.logger.info(
                    f"[{terraform_module_resources.module_name}] Saving Terraform foreach module"
                    f" to {foreach_module_path}"
                )

                self.update_file(
                    file_path=foreach_module_path,
                    file_data=foreach_module_json,
                    encode_with_json=True,
                )

                update_module_dir(foreach_module_path)

            module_path = terraform_module_resources.get_module_path()
            self.logger.info(
                f"[{terraform_module_resources.module_name}] Saving Terraform module to {module_path}"
            )

            self.update_file(
                file_path=module_path,
                file_data=terraform_module_resources.get_mixed(),
                encode_with_json=True,
            )

            update_module_dir(module_path)

        self.logged_statement(
            f"New Terraform library modules: {new_tf_library_modules}"
        )

        orphan_tf_library_modules = (
            set(existing_tf_library_modules) - new_tf_library_modules
        )

        for library_module in orphan_tf_library_modules:
            library_module_dir = self.local_path(library_module).parent
            self.logger.warning(
                f"Deleting orphan library module directory {library_module_dir}"
            )
            self.delete_dir(dir_path=library_module_dir, exit_on_completion=False)

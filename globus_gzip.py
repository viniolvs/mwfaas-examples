import os
import sys
from typing import List
from mwfaas.master import Master
from mwfaas.globus_compute_manager import GlobusComputeCloudManager
from mwfaas.list_distribuition_strategy import ListDistributionStrategy


def compress_files(files_bytes: List[bytes]) -> bytes:
    import gzip

    if not files_bytes or len(files_bytes) == 0:
        return b""
    return gzip.compress(files_bytes[0])


def main():
    original_files = sys.argv[1:]
    input_bytes = []
    try:
        for path in original_files:
            with open(path, "rb") as f:
                input_bytes.append(f.read())
    except FileNotFoundError as e:
        print(f"\nERRO: Arquivo não encontrado: {e.filename}")
        print(
            "Por favor, verifique se todos os caminhos de arquivo estão corretos e tente novamente."
        )
        sys.exit(1)

    print(
        f"Dados de entrada: {len(input_bytes)} arquivos de {len(input_bytes[0])} bytes cada."
    )

    with GlobusComputeCloudManager(auto_authenticate=True) as cloud_manager:
        distribuition = ListDistributionStrategy()
        master = Master(
            cloud_manager=cloud_manager, distribution_strategy=distribuition
        )

        try:
            results = master.run(data_input=input_bytes, user_function=compress_files)
            if results:
                output_dir = "compressed_files_bytes"
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)

                original_file_idx = 0
                for result in results:
                    if original_file_idx < len(original_files):
                        original_filename = os.path.basename(
                            original_files[original_file_idx]
                        )
                        output_path = os.path.join(
                            output_dir, f"{original_filename}.gz"
                        )
                        if isinstance(result, Exception):
                            print(
                                f"  - Tarefa para '{original_filename}' FALHOU: {result}"
                            )
                        else:
                            # 'wb' para escrever em modo binário
                            with open(output_path, "wb") as f:
                                f.write(result)
                            print(
                                f"  - Tarefa para '{original_filename}' SUCESSO: Arquivo salvo em '{output_path}'"
                            )

                        original_file_idx += 1

        except Exception as e:
            print(
                f"\nOcorreu um erro durante a execução do master.run: {type(e).__name__} - {e}"
            )

        print("\n" + "-" * 15 + " Status das Tarefas " + "-" * 15)
        task_statuses = master.get_task_statuses()
        if task_statuses:
            for status in task_statuses:
                status_info = (
                    f"  ID da Tarefa: {status.get('id', 'N/A'):<38} "
                    f"Índice do Bloco: {status.get('chunk_index', 'N/A'):<3} "
                    f"Status: {status.get('status', 'N/A'):<20}"
                )
                print(status_info)
        else:
            print("Nenhum status de tarefa disponível.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "set_endpoints":
        with GlobusComputeCloudManager(auto_authenticate=True) as cloud_manager:
            cloud_manager.configure_endpoints_interactive_and_save()
    elif len(sys.argv) > 1 and sys.argv[1] == "login":
        GlobusComputeCloudManager.login_interactive()
    elif len(sys.argv) > 1 and sys.argv[1] == "logout":
        GlobusComputeCloudManager.logout()
    elif len(sys.argv) < 2:
        print("globus_gzip_example <file1> [file2] [file3] ...")
        sys.exit(1)
    else:
        main()

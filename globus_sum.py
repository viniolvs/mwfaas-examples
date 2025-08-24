import time
import sys
from mwfaas.master import Master
from mwfaas.globus_compute_manager import GlobusComputeCloudManager
from mwfaas.list_distribuition_strategy import ListDistributionStrategy


def somar_bloco_lista(bloco_de_numeros):
    """
    Calcula a soma dos números em uma lista (bloco) fornecida.
    Esta função é executada no 'escravo'.
    Para fins de demonstração, ela também imprime o bloco que está processando.
    """
    soma_bloco = sum(bloco_de_numeros)
    import os

    print(
        f"[Escravo PID: {os.getpid()}] Processando bloco: {bloco_de_numeros[:3]}...{bloco_de_numeros[-3:] if len(bloco_de_numeros) > 3 else ''} (tamanho: {len(bloco_de_numeros)}), Soma do bloco: {soma_bloco}"
    )

    time.sleep(0.5)
    return soma_bloco


def reduce_results(results):
    return sum(results)


def main():
    print("Iniciando exemplo Master-Slave FaaS...")
    print("-" * 40)

    lista_completa_de_numeros = list(
        range(1, 101)
    )  # Lista de 1 a 100. Soma total = 5050.
    print(
        f"Dados de entrada: uma lista de {len(lista_completa_de_numeros)} números (1 a 100)."
    )

    with GlobusComputeCloudManager(auto_authenticate=True) as cloud_manager:
        distribuition = ListDistributionStrategy()
        master = Master(
            cloud_manager=cloud_manager, distribution_strategy=distribuition
        )

        print(f"\nMaster inicializado: {master}")
        print(f"  Cloud Manager em uso: {cloud_manager.__class__.__name__}")
        print(
            f"  Estratégia de Distribuição: {master.distribution_strategy.__class__.__name__}"
        )
        print(
            f"  Paralelismo Alvo (do CloudManager): {cloud_manager.get_worker_count()}"
        )

        print(
            f"\nIniciando processamento com a função '{somar_bloco_lista.__name__}'..."
        )
        try:
            results = master.run(
                data_input=lista_completa_de_numeros, user_function=somar_bloco_lista
            )

            print("\n" + "-" * 15 + " Resultados Recebidos " + "-" * 15)
            if results:
                soma = master.reduce(results, reduce_results)
                print(f"  Soma Total: {soma}")

            else:
                print("Nenhum resultado foi retornado.")

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
                if status.get("status") == "completed" and "result" in status:
                    status_info += f" Resultado: {status['result']}"
                elif (
                    status.get("status") in ["failed", "submission_failed"]
                    and "error" in status
                ):
                    status_info += f" Erro: {type(status['error']).__name__}"
                print(status_info)
        else:
            print("Nenhum status de tarefa disponível.")

    print("\nExemplo finalizado.")
    print("-" * 40)


if __name__ == "__main__":
    main()

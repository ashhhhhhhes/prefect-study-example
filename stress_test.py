from prefect import flow, task
import socket
import multiprocessing
import time
import os


@task(name="CPU 부하 생성 태스크")
def cpu_stress_task(core_id: int, duration_sec: int):
    pid = os.getpid()
    print(f"🔥 [Core {core_id}] {duration_sec}초 부하 시작 (PID: {pid})")

    start_time = time.time()
    # 실제 CPU 점유를 위한 연산 루프
    while time.time() - start_time < duration_sec:
        _ = 100 * 100

    print(f"✅ [Core {core_id}] 테스트 완료")
    return f"Core {core_id} 완료"


@flow(name="Worker Stress Test", log_prints=True)
def stress_test_flow(stress_seconds: int = 60, split_count: int = None):
    host_name = socket.gethostname()
    # 실행 환경의 CPU 코어 수 감지 (지정 안 하면 전체 코어 사용)
    cpu_count = split_count or multiprocessing.cpu_count()

    print(f"--- 병렬 부하 테스트 시작 ---")
    print(f"📍 실행 인스턴스: {host_name}")
    print(f"🧵 생성할 태스크 수 (코어 수): {cpu_count}")
    print(f"⏱️ 각 태스크당 부하 시간: {stress_seconds}초")

    # 1. 태스크들을 리스트에 담아 생성 (병렬 실행 준비)
    results = []
    for i in range(cpu_count):
        # .submit()을 사용하면 비동기(병렬)로 실행됩니다.
        res = cpu_stress_task.submit(core_id=i, duration_sec=stress_seconds)
        results.append(res)

    # 2. 모든 태스크가 완료될 때까지 대기
    for r in results:
        r.wait()

    print(f"✅ 모든 {cpu_count}개의 태스크 종료")


if __name__ == "__main__":
    stress_test_flow(stress_seconds=30)
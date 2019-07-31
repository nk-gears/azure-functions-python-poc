from modules import StopWatch

with StopWatch() as sw:
    print("[*] Elapsed: {0:.2f}s".format(sw.elapsed_s))
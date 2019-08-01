try:
    from SharedCode import StopWatch

    with StopWatch() as sw:
        x=1
except Exception as ex:
    print(ex)
else:
    print("[*] Elapsed: {0:.2f}s".format(sw.elapsed_s))
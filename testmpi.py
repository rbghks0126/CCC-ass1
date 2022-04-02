from mpi4py import MPI
import pandas as pd

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    data = {'a': 7, 'b': 3.14}
    df = pd.DataFrame.from_dict(data, 'index')
    comm.send(df, dest=3, tag=11)
    
elif rank == 3:
    data = comm.recv(source=0, tag=11)
    print(data)
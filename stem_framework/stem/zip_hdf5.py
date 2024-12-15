import numpy as np
import h5py
from zipfile import ZipFile


def zip_to_hdf5(zip_path: str, hdf_path: str) -> None:
    head_size = 24  # bytes
    data_bsize = 1024 * np.dtype('float32').itemsize  # n float32 values size in bytes
    with ZipFile(zip_path, 'r') as zip_obj:
        adc_channels = zip_obj.namelist()
        n_rows = zip_obj.filelist[0].file_size // (data_bsize + head_size)
        with h5py.File(hdf_path, 'w') as hdf_obj:
            data = hdf_obj.create_dataset('converted', (n_rows, len(adc_channels), 1024), 'float32')
            for col, channel_fnm in enumerate(adc_channels):
                with zip_obj.open(channel_fnm) as file:
                    for id_column in range(n_rows):
                        file.seek(24, 1)
                        data[id_column, col, :] = np.frombuffer(file.read(data_bsize), 'float32')

# test:
# if __name__ == '__main__':
#   zip_to_hdf5('../data/wave.dat.zip', '../data/test.hdf5')

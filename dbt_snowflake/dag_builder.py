import os
import sys

levels = int(sys.argv[1])

# extract the current filepath
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
FAKE_MODELS_SUBFOLDER = 'models/fake_dag'
FAKE_MODELS_PATH = os.path.join(DIR_PATH, FAKE_MODELS_SUBFOLDER)


def create_directory():
    try:
        os.makedirs(FAKE_MODELS_PATH, exist_ok = True)
        print("Directory '%s' created successfully" % FAKE_MODELS_SUBFOLDER)
    except OSError as error:
        print("Directory '%s' can not be created" % FAKE_MODELS_SUBFOLDER)
            
def create_nodes(levels):
    if levels < 0:
        return print('Error: Level 0 is the lowest depth')
    prefix = 0
    filenames = []
    while levels > -1:
        for i in range(2**levels):
            suffix = i+1
            file_name = f'_{prefix}__{suffix}.sql'
            filenames.append(file_name)
            if prefix == 0:
                with open(os.path.join(FAKE_MODELS_PATH, file_name), 'w') as fp:
                    fp.write('select 1')
            else:
                with open(os.path.join(FAKE_MODELS_PATH, file_name), 'w') as fp:
                    ref_1 = filenames.pop(0).split('.',1)[0]
                    ref_2 = filenames.pop(0).split('.',1)[0]
                    open_ref = "{{ ref('"
                    close_ref = "') }}"
                    fp.write(f'select * from {open_ref}{ref_1}{close_ref}' '\n  union all \n' f'select * from {open_ref}{ref_2}{close_ref}')
        levels -= 1
        prefix += 1
    print('Created files in the `fake_dag` subfolder!')

if __name__ == '__main__':
    create_directory()
    create_nodes(levels)

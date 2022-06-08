import json
import csv
import glob
import os





def main():
    # flattening function
    def flatten_json(y):
        out = {}

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(y)
        return out
    cwd = os.getcwd()
    data_path=os.path.join(cwd,'data')
    search_path=data_path+'/**/*.json'
    print('using glob')
    files=glob.glob(search_path,recursive=True)
    for file in files:
        filename=file.split('.')[0]
        csv_file_path=filename+'.csv'
        print(csv_file_path)
        with open(file) as json_file:
            data=json.load(json_file)
        flat=flatten_json(data)
        print(flat)
        with open(csv_file_path,'w') as data_file:
            header=flat.keys()
            wr=csv.DictWriter(data_file,fieldnames=header,lineterminator='\n')
            wr.writeheader()
            wr.writerow(flat)
    pass


if __name__ == '__main__':
    main()

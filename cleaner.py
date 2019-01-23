from PIL import Image
import urllib.request
import os 
import multiprocessing
import time
import random
import pandas as pd

write_lock = multiprocessing.Lock()

other = [3,8,10,11,14,15,18, 20,25,27, 28,29, 30,31]

def create_dataframe(exclude=[]):
    df = pd.read_csv('books.csv',encoding="latin",header=None)
    df.columns = ['img_idx','img_file','img_link','title','author','cat_id','category']
    df = df.loc[~df['cat_id'].isin(exclude)]
    return df

cat_id_dict = create_dataframe().groupby('category').min()['cat_id'].sort_values()

def get_categories(df):
    return df.groupby('category').min()['cat_id'].sort_values()

def build_images_linear(df, thread_id=0):   
    print('thread_id: ', thread_id, ' starting')
    i_imported = 0
    for index, row in df.iterrows():
        filename = 'pictures/{}'.format(row["img_file"])
        if not os.path.isfile(filename):
            import_and_resize(row)
            i_imported += 1
    print('thread_id: ', thread_id, ' finishing')
    print('thread_id: ', thread_id, 'imported ', i_imported, ' new images')


def build_images_parallel(df, num_threads):
    if __name__ == '__main__':
       print('Generating images with ', num_threads, ' concurrent processes')
       num_rows = df.shape[0]
       batch_size = int(num_rows/num_threads)
       print('batch size is: ', batch_size)
       for i in range(num_threads):
          df_i = df.loc[i*batch_size : min(df.shape[0], (i+1)*batch_size)]
          print(df_i.head())
          print(df_i.tail())
          p = multiprocessing.Process(target=build_images_linear, args=(df_i,i))
          p.start()
          

def import_and_resize(df_entry, width=299, height=299):
    filename = 'pictures/{}'.format(df_entry["img_file"])

    #pull file
    urllib.request.urlretrieve(df_entry["img_link"], filename=filename)

    #resize file to 250x250 and stretch
    with open(filename, 'r+b') as f:
        with Image.open(f) as img:
            new_img = img.resize((width,height))
            new_img = new_img.convert("RGB")
            write_lock.acquire()
            new_img.save(filename, "JPEG", optimize=True)
            write_lock.release()

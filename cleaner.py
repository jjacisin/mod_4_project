from PIL import Image
import urllib.request

def build_images(df):
    for index, row in df.iterrows():
        import_and_resize(row)

def import_and_resize(df_entry, width=250, height=250):
    filename = 'pictures/{}'.format(df_entry["img_file"])

    #pull file
    urllib.request.urlretrieve(df_entry["img_link"], filename=filename)

    #resize file to 250x250 and stretch
    with open(filename, 'r+b') as f:
        with Image.open(f) as img:
            new_img = img.resize((width,height))
            new_img.save(filename, "JPEG", optimize=True)

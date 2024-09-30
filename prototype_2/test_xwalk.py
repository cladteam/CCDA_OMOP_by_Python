
from foundry.transforms import Dataset
# import org.apache.hadoop.fs


# Junk is on the master branch
test_output_ds = Dataset.get("test_output_ds").read_table(format="pandas")
print(test_output_ds)


# fails because I can only add this file as raw, I suspect b/c it isn't in the master branch.
#concept_xwalk = Dataset.get("concept_xwalk")
#xwalk_ds = concept_xwalk.read_table(format="pandas")
#print(xwalk_ds)

exit()



## Monkeying around trying to read as  a raw file...
print(type(concept_xwalk))
print(concept_xwalk)
# <class 'foundry.transforms._dataset.Dataset'>
#<foundry.transforms._dataset.Dataset object at 0x7f4e5475afd0>
print(dir(concept_xwalk))


concept_xwalk_files = concept_xwalk.files().download()
#print(type(concept_xwalk_files))
#print(concept_xwalk_files)

print(concept_xwalk._path_to_downloaded_files)

for xwalk_file in concept_xwalk.files():
    print(xwalk_file)
    print(dir(xwalk_file))
    print(xwalk_file.path)
    x = xwalk_file.download()
    print("")
    print ("---- after download() ----")
    import locale
    print(locale.getpreferredencoding())
    print(type(x))
    print(dir(x))
    print(x)
    print("  ")
    #with open(x, encoding="utf-8") as file_x: # barfs
    #with open(x, encoding="latin1") as file_x:  # prints 1/2 legible garbage
    with open(x) as file_x:
        for thing in file_x:
            print(thing)

#import pandas as pd
#concept_df = pd.read_csv("map_to_standard.csv")
#print(concept_df)

#########################################

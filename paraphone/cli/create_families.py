import sys
import argparse
from os.path import join
sys.path.insert(0, "./src/main/python")
from Trainset import Trainset
parser = argparse.ArgumentParser(
        description='Welcome to Families',
        prog="Families",
        allow_abbrev=False,
        add_help=True
#         fromfile_prefix_chars='@',
    )
parser.add_argument(
    "dataset",
    help="dataset where find texts and index"
)
parser.add_argument(
    "--output",
    help="folder to output in",
    action="store",
    type=str
)
parser.add_argument(
    "--n_fam",
    help="number of families",
    action="store",
    type=int
)
parser.add_argument(
    "--lang",
    help="en or fr",
    action="store",
    type=str,
    choices= ["en", "fr"]
)
args = parser.parse_args()

ts = Trainset(args.dataset, args.lang )
_ , results = ts.split_in_families(args.n_fam)
with open(join(args.output, f"cr_{str(args.n_fam)}_{args.lang}.txt"),"w") as f:
    for i, fam in enumerate(results):
        for text in fam:
            text = text[0]
            f.write(f"{str(i)}\t{text}\n")
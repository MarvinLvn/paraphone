from os.path import exists, isdir, join, basename, dirname

import pandas


class Trainset:
    """
    Read matched2.csv and generate speaker's familly according to number wanted
    
    Parameters
    ----------
    
    dataset : str
       path to dataset
    false_directory : str
        delete the first part of transcription path as a 'basename'
    
    
    Notes
    -----
    
    Two directories|files must be in path : 
    - `dataset/metadata/matched2.csv`
    - `dataset/text/`
    matched2.csv file must be a csv file with `comma` separator
    It must have at least two columns : `text_path` and `family_id`
    
    Raises
    ------
    NotADirectoryError
        If the path to dataset is not a directory or does not exists
    FileNotFoundError
        If the matched file is not found or text files mentionned in matched2.csv file does not exist
    
    """

    def __init__(self, dataset, train="en"):

        if not isdir(dataset) or not isinstance(dataset, str):
            raise NotADirectoryError("{} is not a directory".format(dataset))
        if not exists(dataset):
            raise NotADirectoryError("{} is not a directory".format(dataset))

        self.matched = join(dataset, "metadata/matched2.csv")
        if not exists(self.matched):
            raise FileNotFoundError("{} does not exists".format(self.matched))

        datas = pandas.read_csv(self.matched, header=0)

        self.data = []
        for index, row in datas.iterrows():
            if row["language"] == train and train == "en":
                text_path = join(dataset, "text/EN/LibriVox/", basename(row["text_path"]))
                if not exists(text_path):
                    raise FileNotFoundError("{} does not exists".format(text_path))
                self.data.append((text_path, int(row["family_id"])))
            elif row["language"] == train and train == "fr":
                if "litteratureaudio" in row["text_path"]:
                    text_path = join(dataset, "text/FR/LittAudio/", basename(dirname(row["text_path"])),
                                     basename(row["text_path"]))
                elif "LirbiVox" in row["text_path"]:
                    text_path = join(dataset, "text/FR/LibriVox/", basename(row["text_path"]))
                if not exists(text_path):
                    raise FileNotFoundError("{} does not exists".format(text_path))
                self.data.append((text_path, int(row["family_id"])))
        self.data = sorted(self.data, key=lambda tup: tup[1])
        self.paths = [i[0] for i in self.data]
        self.fams = [i[1] for i in self.data]
        self.max_ds = 64

    def split_in_families(self, nf, begin=64):
        """
        Generate families by concataination of following families
        
        Parameters
        ----------
        
        nf : int
            the number of families wanted
        begin : int
            the number of families at this level
            
        Returns
        -------
        
        tuple
          bool and list of level families containing a list of path to book and original family of the book
          
        Examples
        --------
        
        >>> l_fam
            [
             [('path/to/book1', 0), ('path/to/book2', 1)],
             [('path/to/book3', 2), ('path/to/book4', 3)]
            ]
        """

        # prepare to return l_fam
        l_fam = [[] for i in range(nf)]
        qtt = int(self.max_ds / nf)
        i = 0
        # for each level's families
        for i in range(0, begin, qtt):
            # for the number of original families in level's families
            for j in range(0, qtt):
                # take the number of the original family
                k = i + j
                # find indexes of this family from matched file
                indexs = [index for index, value in enumerate(self.fams) if value == k]
                # records the book's path and it's orginal family in level's family
                [l_fam[int(i / qtt)].append(self.data[id]) for id in indexs]
        return (True, l_fam)

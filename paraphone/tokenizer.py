from re import split, sub


class Tokenizer:
    """
    Reads a corpus. Takes in entry a list of 'families'; ie, a list of lists of text's path
    ready to tokenize in words.
    Returns a list of words that is the intersection between the families
    
    Parameters
    ----------
    
    corpus : list
        a list of list of string (text's path)
    rx : str
        a regular expression to define which characters must be removed
        
    Returns
    -------
    
    list : 
        a list of ords that ins the intersection between the words in the families
    """

    def __init__(self, corpus, rx=[]):
        self.corpus = corpus
        self.rx = rx
        self.d_word = {}

    def tokenize_book(self, book, tiret=True):
        """
        tokenize a file in words according to a regexp
        
        Parameters
        ----------
        
        book : str
            a path to a file to read
            
        Returns
        -------
        
        list : 
            a list of unique words (str)
            
        Notes
        -----
        
        - read non binary file
        - replace '\n' by ' '
        - remove blanck and characters according to the regexp
        - lower case
        - sort words
        """

        def remove_apostrophe(word):
            if word == '':
                return False, word
            if word[0] in ["\"", "'"]:
                word = word[1:]
            if word == '':
                return False, word
            if word[-1] in ["\"", "'"]:
                word = word[:-1]
            if word == '':
                return False, word
            return True, word

        print("tokenize", book)
        with open(book, "r") as f:
            text = f.read()
        text = " ".join(text.split("\n"))
        for rx in self.rx:
            fnd, rplc = rx
            text = sub(fnd, rplc, text)
        to_return = []
        for tokens in split(" ", text):

            if "-" in tokens:

                if tokens.count("-") > 1:
                    tokens = tokens.split('-')
                else:
                    tokens = [tokens.lower()]
            else:
                tokens = [tokens.lower()]
            to_return.extend(tokens)
            for tok in tokens:
                if self.d_word.get(tok, None) is None:
                    self.d_word[tok] = 0
                self.d_word[tok] += 1
        return list(set(to_return))

    def write(self, output="../../wuggy/wug/intersection.txt"):
        with open(output, "w") as f:
            f.write("word,count\n")

    def union_intra_families(self, family):
        """
        join the words inside a family of texts
        
        Parameters
        ----------
        
        family :  list
            a list of text's path (str)
            
        Returns
        -------
        
        list:
            a list of sorted and unique words
            
        Notes
        -----
        
        call 'Tokenization.tokenize_book'
        """
        self.d_word = {}
        to_return = []
        for book in family:
            to_return.extend(self.tokenize_book(book))
            to_return = sorted(list(set(to_return)))
        return to_return

    def union_inter_families(self):
        """
        join the words between families from the corpus
        
        Returns
        -------
        
        list:
            a list of sorted and unique words
            
        Notes
        -----
        
        call 'Tokenization.union_intra_families'
        """

        to_return = []
        for family in self.corpus:
            to_return.extend(self.union_intra_families(family))
            to_return = sorted(list(set(to_return)))
        return to_return

    def intersect_inter_families(self):
        """
        find the intersection between words of differents families of texts
        
        Returns
        -------
        
        list:
            a list of sorted and unique words
            
        Notes
        -----
        
        - call 'Tokenization.union_inter_families' for a complete list of words
        - for each family
        - call 'Tokenization.union_intra_families' for a list of words inside a family
        - find the intersection between families
        """
        ######try to pass onely one time!
        to_return = set(self.union_intra_families(self.corpus[0]))
        for family in self.corpus[1:]:
            to_return = to_return & set(self.union_intra_families(family))
        return sorted(list(to_return))

'''
Write token counts in the form "id   unigram   count" per volume in the HTRC feature extraction dataset.
'''
import os
import argparse
import logging
import pandas as pd
import bz2
import json
import sys

def load_pages(path):
    f = bz2.BZ2File(path)
    rawjson = f.readline()
    voljson = json.loads(rawjson)
    pages = voljson['features']['pages']
    f.close()
    return pages
    
def get_feature_df(pages, filename):
    df_inputlist = []
#    logging.debug(filename)
    volume_text = ""
    for page in pages:
#        logging.debug(page)
        seq = page['seq']
        try:
            tokens = page['body']['tokenPosCount']
        #OLD FORMAT: tokens = page['body']['tokens']
            for (token, poscounts) in tokens.items():
                for pos, count in poscounts.items():
                    volume_text = " ".join([volume_text," ".join([token]*count)])
#                    df_inputlist += [{"filename": filename[:-9], "seq":seq, "token": token, "pos": pos, "freq": count}]
        except TypeError:
            pass
    df_inputlist = [{"filename": filename[:-9], "text": volume_text}]
    df = pd.DataFrame(df_inputlist)
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', nargs='+')
    parser.add_argument('--logpath', '-l', type=str, help='Location for the logfile', default="featurereader.log")
    args = parser.parse_args()
    logging.basicConfig(filename=args.logpath, level=logging.DEBUG)
    args = parser.parse_args()
    logging.basicConfig(filename="featurecount.log", level=logging.DEBUG)
    logging.debug("Starting")
    
    for path in args.path:
        filename = os.path.basename(path).split(".basic.json")[0]

        try:
            pages = load_pages(path)
            df = get_feature_df(pages, filename)
        except Exception as e:
            logging.exception("Error loading %s" % path)
            continue
        
        logging.debug("Midpoint")

        if len(df) == 0:
            logging.info("%s does not seem to have any data" % path)
            continue
        try:
#            volume_level_counts = df.groupby(['filename', 'token']).sum().loc[:,"freq"]
#            logging.debug(sys.stdout)
            logging.debug(df['filename'])
            df.to_csv(sys.stdout, sep="\t", encoding='utf-8', index=False, header=False)
#            logging.debug(volume_level_counts.to_csv(sep="\t", encoding='utf-8'))
#            volume_level_counts.to_csv(sys.stdout, sep="\t", encoding='utf-8')
        except Exception as e:
            logging.debug(df)
            logging.exception("problem while parsing %s" % filename)


if __name__=='__main__':
    main()

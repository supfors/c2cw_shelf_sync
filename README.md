# run.py
This script attempts to create [calibre-web](https://github.com/janeczku/calibre-web) shelves for each tag in the [calibre](https://calibre-ebook.com/) database and then associate books based on their respective tags and links in calibre. I'm using it so my tags show up as collections on my Kobo E-reader. This script was primarily a learning experience with pandas, use at your own risk ;-).

# Getting started
:warning: **Make sure you have a back-up of your calibre-web database before you start.**
* The script needs access to both databases, typically `metadata.db` for calibre, and `app.db` for calibre-web, update the paths to `calibre_db` and `calibre_web_db` in `run.py`.
* Install pandas: `pip3 install pandas`.
* Run the script, optionally add logging with `--log`

# Notes
* The script does not touch shelves for which no corresponding tag(name) is found in the calibre database.
* The script removes books from a shelf when you remove a tag from a book in calibre. (Previous note also applies here)
* The first calibre-web login after the initial sync may take some time.

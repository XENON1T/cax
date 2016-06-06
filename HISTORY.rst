=======
History
=======

2.0.0 (2016-06-06)
------------------

* Use different folder for raw and root data

* Add cax-mv, cax-rm, cax-stray

* Don't need to clear DAQ buffer anymore in cax.

* Cleanup and fixes related to processing.

* Transfer bug that made bad element in data location list

* Specify the partition in qsub.py

* filesystem.py: Add a class to ask for the status of a file or folder

1.2.0 (2016-5-26)
------------------

* Retry if errored instead of waiting two days.

1.1.2 (2016-5-26)
------------------

* Specify log level on command line.

1.1.1 (2016-5-26)
------------------

* Version number only in file log, not screen

1.1.0 (2016-5-26)
------------------

* Add release support
* Add version number to log output

1.0.0 (2016-5-26)
------------------

* Initial stable release
* SCP support for transfer
* Checksumming
* Retry failed transfers if checksum fails or timeout
* Processing on batch queue

0.1.0 (2016-1-22)
------------------

* Initial release

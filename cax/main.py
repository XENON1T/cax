import logging
import sys
import os

from cliff.app import App
from cliff.commandmanager import CommandManager


class CopyingAroundXENON1TApp(App):

    log = logging.getLogger(__name__)

    def __init__(self):
        super(CopyingAroundXENON1TApp, self).__init__(
            description='cax demo app',
            version='0.1.0',
            command_manager=CommandManager('cax.app'),
            )

    def initialize_app(self, argv):
        if os.environ.get('MONGO_PASSWORD') is None:
            raise RuntimeError('Environmental variable MONGO_PASSWORD not set.'
                               ' This is required for communicating with the '
                               'run database.  To fix this problem, Do:'
                               '\n\n\texport MONGO_PASSWORD=xxx\n\n'
                               'Then rerun this command.')

    def prepare_to_run_command(self, cmd):
        self.log.debug('prepare_to_run_command %s', cmd.__class__.__name__)

    def clean_up(self, cmd, result, err):
        self.log.debug('clean_up %s', cmd.__class__.__name__)
        if err:
            self.log.debug('got an error: %s', err)


def main(argv=sys.argv[1:]):
    myapp = CopyingAroundXENON1TApp()
    return myapp.run(argv)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
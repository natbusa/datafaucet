"""The root `datafaucet` command.
This does nothing other than dispatch to subcommands or output path info.
"""

import argparse
import errno
import os
import sys
from subprocess import Popen

from .._version import __version__


class DatalabframeworkParser(argparse.ArgumentParser):

    @property
    def epilog(self):
        """Add subcommands to epilog on request

        Avoids searching PATH for subcommands unless help output is requested.
        """
        return 'Available subcommands: %s' % ', '.join(list_subcommands())

    @epilog.setter
    def epilog(self, x):
        """Ignore epilog set in Parser.__init__"""
        pass


def datafaucet_parser():
    parser = DatalabframeworkParser(
        description="Datafaucet: Notebook Milling",
    )
    group = parser.add_mutually_exclusive_group(required=True)

    # don't use argparse's version action because it prints to stderr on py2
    group.add_argument('--version', action='store_true', help="show the datafaucet command's version and exit")
    group.add_argument('subcommand', type=str, nargs='?', help='the subcommand to launch')

    return parser


def list_subcommands():
    """List all datafaucet subcommands

    searches PATH for `datafaucet-name`

    Returns a list of datafaucet's subcommand names, without the `datafaucet-` prefix.
    Nested children (e.g. datafaucet-sub-subsub) are not included.
    """
    subcommand_tuples = set()
    # construct a set of `('foo', 'bar') from `datafaucet-foo-bar`
    for d in _path_with_self():
        try:
            names = os.listdir(d)
        except OSError:
            continue
        for name in names:
            if name.startswith('datafaucet-'):
                if sys.platform.startswith('win'):
                    # remove file-extension on Windows
                    name = os.path.splitext(name)[0]
                subcommand_tuples.add(tuple(name.split('-')[1:]))
    # build a set of subcommand strings, excluding subcommands whose parents are defined
    subcommands = set()
    # Only include `datafaucet-foo-bar` if `datafaucet-foo` is not already present
    for sub_tup in subcommand_tuples:
        if not any(sub_tup[:i] in subcommand_tuples for i in range(1, len(sub_tup))):
            subcommands.add('-'.join(sub_tup))
    return sorted(subcommands)


def _execvp(cmd, argv):
    """execvp, except on Windows where it uses Popen

    Python provides execvp on Windows, but its behavior is problematic (Python bug#9148).
    """
    if sys.platform.startswith('win'):
        # PATH is ignored when shell=False,
        # so rely on shutil.which
        from shutil import which
        cmd_path = which(cmd)
        if cmd_path is None:
            raise OSError('%r not found' % cmd, errno.ENOENT)
        p = Popen([cmd_path] + argv[1:])
        # Don't raise KeyboardInterrupt in the parent process.
        # Set this after spawning, to avoid subprocess inheriting handler.
        import signal
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        p.wait()
        sys.exit(p.returncode)
    else:
        os.execvp(cmd, argv)


def _path_with_self():
    """Put `datafaucet`'s dir at the front of PATH

    Ensures that /path/to/datafaucet subcommand
    will do /path/to/datafaucet-subcommand
    even if /other/datafaucet-subcommand is ahead of it on PATH
    """
    scripts = [sys.argv[0]]
    if os.path.islink(scripts[0]):
        # include realpath, if `datafaucet` is a symlink
        scripts.append(os.path.realpath(scripts[0]))

    path_list = (os.environ.get('PATH') or os.defpath).split(os.pathsep)
    for script in scripts:
        bindir = os.path.dirname(script)
        if os.path.isdir(bindir) and os.access(script, os.X_OK):   # only if it's a script
            # ensure executable's dir is on PATH
            # avoids missing subcommands when datafaucet is run via absolute path
            path_list.insert(0, bindir)
    os.environ['PATH'] = os.pathsep.join(path_list)
    return path_list


def main():
    _path_with_self()  # ensure executable is on PATH
    if len(sys.argv) > 1 and not sys.argv[1].startswith('-'):
        # Don't parse if a subcommand is given
        # Avoids argparse gobbling up args passed to subcommand, such as `-h`.
        subcommand = sys.argv[1]
    else:
        parser = datafaucet_parser()
        args, opts = parser.parse_known_args()
        subcommand = args.subcommand
        if args.version:
            print(__version__)
            return

    if not subcommand:
        parser.print_usage(file=sys.stderr)
        sys.exit("subcommand is required")

    command = 'datafaucet-' + subcommand
    try:
        _execvp(command, sys.argv[1:])
    except OSError as e:
        sys.exit("Error executing Datalabframework command %r: %s" % (subcommand, e))


if __name__ == '__main__':
    main()

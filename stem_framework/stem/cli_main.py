import argparse
import cmd
from stem.workspace import IWorkspace


def print_structure(workspace: IWorkspace, args: argparse.Namespace):
    def pretty(d, indent=0):
        for key, value in d.items():
            print('\t' * indent + str(key))
            if isinstance(value, dict):
                pretty(value, indent + 1)
            else:
                print('\t' * (indent + 1) + str(value))

    pretty(workspace.structure())


def run_task(workspace: IWorkspace, args: argparse.Namespace):
    pass  # TODO()


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run task in workspace')
    parser.add_argument(
        '-w', '--workspace', metavar='WORKSPACE',
        help='Add path to workspace or file for module workspace',
        required=True
    )

    subparsers = parser.add_subparsers(metavar='command')
    structure_parser = subparsers.add_parser('structure', help='Print workspace structure')
    structure_parser.set_defaults(func=print_structure)
    run_parser = subparsers.add_parser('run', help='Run task')
    run_parser.set_defaults(func=run_task)
    run_parser.add_argument('TASKPATH')
    run_parser.add_argument(
        '-m', '--meta',
        metavar='META', help='Metadata for task or path to file with metadata in JSON format'
    )
    return parser


def stem_cli_main():
    parser = create_parser()
    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args.workspace, args)


if __name__ == '__main__':
    stem_cli_main()
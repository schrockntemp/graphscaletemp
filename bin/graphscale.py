import click
# import sys

@click.group()
def cli():
    pass

@click.command(short_help='Generate pents from graphql file')
# @click.argument('file', help='Path to graphql schema file')
@click.argument('file', type=click.Path())
def genpents(file):
    print('path: ' + file)
    fff = open(file, 'r')
    lines = fff.readlines()
    print(lines)

if __name__ == '__main__':
    cli.add_command(genpents)
    cli()

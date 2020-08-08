import yanshuf

from jinja2 import Environment, PackageLoader, select_autoescape

env = Environment(
    loader=PackageLoader('yanshuf', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

template = env.get_template('index.html.jinja')
table = yanshuf.run_tailored_dragon()
print(template.render(table=table))

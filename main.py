import yanshuf

from jinja2 import Environment, PackageLoader, select_autoescape

env = Environment(
    loader=PackageLoader('yanshuf', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

template = env.get_template('index.html')
df = yanshuf.run_dragon()
table = yanshuf.to_html(df)
print(template.render(table=table))

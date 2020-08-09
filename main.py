import yanshuf

from jinja2 import Environment, PackageLoader, select_autoescape

env = Environment(
    loader=PackageLoader('yanshuf', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

template = env.get_template('index.html.jinja')
perf, corr, info = yanshuf.run_tailored_dragon()
print(template.render(perf=perf, corr=corr, info=info))

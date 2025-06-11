"""
This module provides functionality to render SQL templates using the Jinja2
templating engine. It includes a single function, render_sql_template, which
takes a file path to a SQL template and a dictionary of variables, and returns
the rendered SQL query as a string.
"""
from jinja2 import Template


def render_sql_template(template_path, variables):
    """
        Render a SQL template file using Jinja templating engine.

        Args:
            template_path (str): The file path to the SQL template file.
            variables (dict): A dictionary containing the variables to be rendered into the template.

        Returns:
            str: The rendered SQL query as a string.
    """
    with open(template_path, 'r', encoding='utf-8') as file:
        template_content = file.read()
    template = Template(template_content)
    return str(template.render(variables))

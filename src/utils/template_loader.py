import os
from pathlib import Path
from typing import Dict, Any


class TemplateLoader:
    """Utility class for loading and rendering HTML templates"""
    
    def __init__(self, template_dir: str = None):
        if template_dir is None:
            # Default to templates directory relative to this file
            current_dir = Path(__file__).parent
            template_dir = current_dir.parent / "templates"
        
        self.template_dir = Path(template_dir)
    
    def load_template(self, template_name: str) -> str:
        """Load template content from file"""
        template_path = self.template_dir / template_name
        
        if not template_path.exists():
            raise FileNotFoundError(f"Template not found: {template_path}")
        
        with open(template_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """Load template and render with context variables"""
        template_content = self.load_template(template_name)
        return template_content.format(**context)
    
    def load_partial(self, partial_name: str) -> str:
        """Load partial template from partials directory"""
        partial_path = self.template_dir / "partials" / partial_name
        
        if not partial_path.exists():
            raise FileNotFoundError(f"Partial template not found: {partial_path}")
        
        with open(partial_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def render_partial(self, partial_name: str, context: Dict[str, Any]) -> str:
        """Load partial template and render with context variables"""
        partial_content = self.load_partial(partial_name)
        return partial_content.format(**context)
    
    def copy_css_to_output(self, css_filename: str, output_dir: str) -> str:
        """Copy CSS file to output directory"""
        # CSS files are now in src/static/css/
        current_dir = Path(__file__).parent
        css_source = current_dir.parent / "static" / "css" / css_filename
        css_dest = Path(output_dir) / css_filename
        
        if not css_source.exists():
            raise FileNotFoundError(f"CSS file not found: {css_source}")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Copy CSS file
        with open(css_source, 'r', encoding='utf-8') as src:
            with open(css_dest, 'w', encoding='utf-8') as dst:
                dst.write(src.read())
        
        return str(css_dest)
    
    def copy_js_to_output(self, js_filename: str, output_dir: str) -> str:
        """Copy JavaScript file to output directory"""
        # JavaScript files are now in src/static/js/
        current_dir = Path(__file__).parent
        js_source = current_dir.parent / "static" / "js" / js_filename
        js_dest = Path(output_dir) / js_filename
        
        if not js_source.exists():
            raise FileNotFoundError(f"JavaScript file not found: {js_source}")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Copy JavaScript file
        with open(js_source, 'r', encoding='utf-8') as src:
            with open(js_dest, 'w', encoding='utf-8') as dst:
                dst.write(src.read())
        
        return str(js_dest)
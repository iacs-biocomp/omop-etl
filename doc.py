import pdoc
import os
import config

import shutil
shutil.rmtree('./docs')
os.mkdir('./docs')

modules=['Orchestrator']

for r in config.ordered_scripts:
    """ Define the class dynamically"""
    module = modules.append('sources.'+r)

for  root, dirs, files in os.walk('models'):
    for name in files:
        if '.pyc' not in name:
            module = modules.append('models.'+name.split('.')[0])

for  root, dirs, files in os.walk('utils'):
    for name in files:
        if '.pyc' not in name:
            module = modules.append(root.replace('/', '.')+'.'+name.split('.')[0])

        
    

#modules = ['ETL', 'jobs.Condition', 'jobs.subjobs.condition']  # Public submodules are auto-imported
context = pdoc.Context()

modules = [pdoc.Module(mod, context=context) for mod in modules]
pdoc.link_inheritance(context)

def recursive_htmls(mod):
    yield mod.name, mod.html(show_source_code = False, show_inherited_members = True)
    #yield mod.name, mod.html()
    for submod in mod.submodules():
        yield from recursive_htmls(submod)

for mod in modules:
    for module_name, html in recursive_htmls(mod):
        path=''
        if len(module_name.split('.'))> 1:
            path ='docs/'+'/'.join(module_name.split('.')[0:-1])+'/'
            
            from pathlib import Path
            Path(path).mkdir(parents=True, exist_ok=True)
        
        
        f = open(path+module_name.split('.')[-1]+".html", "w", encoding='utf-8')
        f.write(html)
        f.close()




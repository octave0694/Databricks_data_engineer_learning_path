# Databricks notebook source
# MAGIC %md
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Variable Substitutions in DABs
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how variables make Declarative Automation Bundles dynamic and reusable. You will review the built-in substitutions Databricks provides, then learn to define simple, complex, and lookup variables, override variable values per target environment, and understand the benefits of using variables.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC 1. **Use** the default variable substitutions available in a bundle.
# MAGIC 2. **Define** simple custom variables, complex variables, and lookup variables.
# MAGIC 3. **Override** variable values for a specific target environment.
# MAGIC 4. **Explain** the benefits of using variables in DABs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Variables Overview

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Default Substitutions, Simple, and Complex Variables
# MAGIC
# MAGIC Declarative Automation Bundles support **substitutions** and **custom variables**, both of which allow values to be resolved dynamically when the bundle is deployed and run. Reference any of them with `${...}`.
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:18px;max-width:1150px;margin:0 auto;font-family:sans-serif;align-items:start;">
# MAGIC   <div>
# MAGIC     <div style="font-size:15pt;font-weight:700;color:#0b2026;border-bottom:3px solid #FF5F46;padding-bottom:6px;margin-bottom:12px;">Default Substitutions</div>
# MAGIC     <div style="font-size:12.5pt;color:#0b2026;margin-bottom:10px;">By default there are a variety of <b>variable substitutions</b> available:</div>
# MAGIC     <div style="background:#F9F7F4;border-left:5px solid #FF5F46;border-radius:0 6px 6px 0;padding:7px 12px;font-size:12.5pt;color:#0b2026;font-family:Consolas,Monaco,monospace;box-shadow:0 2px 8px rgba(27,49,57,0.06);margin-bottom:8px;">${bundle.name}</div><div style="background:#F9F7F4;border-left:5px solid #FF5F46;border-radius:0 6px 6px 0;padding:7px 12px;font-size:12.5pt;color:#0b2026;font-family:Consolas,Monaco,monospace;box-shadow:0 2px 8px rgba(27,49,57,0.06);margin-bottom:8px;">${bundle.target}</div><div style="background:#F9F7F4;border-left:5px solid #FF5F46;border-radius:0 6px 6px 0;padding:7px 12px;font-size:12.5pt;color:#0b2026;font-family:Consolas,Monaco,monospace;box-shadow:0 2px 8px rgba(27,49,57,0.06);margin-bottom:8px;">${workspace.file_path}</div><div style="background:#F9F7F4;border-left:5px solid #FF5F46;border-radius:0 6px 6px 0;padding:7px 12px;font-size:12.5pt;color:#0b2026;font-family:Consolas,Monaco,monospace;box-shadow:0 2px 8px rgba(27,49,57,0.06);margin-bottom:8px;">${workspace.root_path}</div><div style="background:#F9F7F4;border-left:5px solid #FF5F46;border-radius:0 6px 6px 0;padding:7px 12px;font-size:12.5pt;color:#0b2026;font-family:Consolas,Monaco,monospace;box-shadow:0 2px 8px rgba(27,49,57,0.06);margin-bottom:8px;">${resources.jobs.&lt;job-name&gt;.id}</div><div style="background:#F9F7F4;border-left:5px solid #FF5F46;border-radius:0 6px 6px 0;padding:7px 12px;font-size:12.5pt;color:#0b2026;font-family:Consolas,Monaco,monospace;box-shadow:0 2px 8px rgba(27,49,57,0.06);margin-bottom:8px;">${resources.models.&lt;model-name&gt;.name}</div><div style="background:#F9F7F4;border-left:5px solid #FF5F46;border-radius:0 6px 6px 0;padding:7px 12px;font-size:12.5pt;color:#0b2026;font-family:Consolas,Monaco,monospace;box-shadow:0 2px 8px rgba(27,49,57,0.06);margin-bottom:8px;">${resources.pipelines.&lt;pipeline-name&gt;.name}</div>
# MAGIC   </div>
# MAGIC   <div>
# MAGIC     <div style="font-size:15pt;font-weight:700;color:#0b2026;border-bottom:3px solid #4299E0;padding-bottom:6px;margin-bottom:12px;">Simple Custom Variables</div>
# MAGIC     <ul style="font-size:12.5pt;color:#0b2026;padding-left:18px;margin:0 0 10px 0;line-height:1.45;">
# MAGIC       <li>You can define <b>simple custom variables</b> in your bundle to enable dynamic retrieval of values needed for many scenarios.</li>
# MAGIC       <li>Custom variables are declared in your bundle configuration files within the <b>variables</b> mapping.</li>
# MAGIC     </ul>
# MAGIC     <div class="code-block" data-language="yaml">
# MAGIC variables:
# MAGIC   my_lab_user_name:
# MAGIC     description: Your user name
# MAGIC     default: labuser23904
# MAGIC </div>
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-yaml.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-bash.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC     if (block.getAttribute('data-processed')) return;
# MAGIC     block.setAttribute('data-processed','true');
# MAGIC     var lang=block.getAttribute('data-language')||'yaml'; var c=block.textContent.trim();
# MAGIC     var id='c-'+Math.random().toString(36).substr(2,9);
# MAGIC     block.innerHTML='<div style="position:relative;margin:12px 0;"><button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button><pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="'+id+'" class="language-'+lang+'" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code></pre></div>';
# MAGIC     var ce=document.getElementById(id); ce.textContent=c; Prism.highlightElement(ce);
# MAGIC     block.querySelector('.copy-btn').onclick=function(){var t=document.createElement('textarea');t.value=c;document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='✓ Copied!';setTimeout(()=>this.textContent='Copy',2000);};
# MAGIC   });
# MAGIC })();
# MAGIC </script>
# MAGIC   </div>
# MAGIC   <div>
# MAGIC     <div style="font-size:15pt;font-weight:700;color:#0b2026;border-bottom:3px solid #00A972;padding-bottom:6px;margin-bottom:12px;">Complex Variables</div>
# MAGIC     <ul style="font-size:12.5pt;color:#0b2026;padding-left:18px;margin:0 0 10px 0;line-height:1.45;">
# MAGIC       <li>A custom variable is assumed to be of type <b>string</b> unless you define it as a complex variable.</li>
# MAGIC       <li>Define a custom variable by setting the <b>type</b> to <code>complex</code>.</li>
# MAGIC     </ul>
# MAGIC     <div class="code-block" data-language="yaml">
# MAGIC variables:
# MAGIC   my_cluster:
# MAGIC     description: "My cluster"
# MAGIC     type: complex
# MAGIC     default:
# MAGIC       spark_version: "15.4.x-scala2.11"
# MAGIC       node_type_id: "Standard_DS3_v2"
# MAGIC       num_workers: 2
# MAGIC </div>
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-yaml.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-bash.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC     if (block.getAttribute('data-processed')) return;
# MAGIC     block.setAttribute('data-processed','true');
# MAGIC     var lang=block.getAttribute('data-language')||'yaml'; var c=block.textContent.trim();
# MAGIC     var id='c-'+Math.random().toString(36).substr(2,9);
# MAGIC     block.innerHTML='<div style="position:relative;margin:12px 0;"><button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button><pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="'+id+'" class="language-'+lang+'" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code></pre></div>';
# MAGIC     var ce=document.getElementById(id); ce.textContent=c; Prism.highlightElement(ce);
# MAGIC     block.querySelector('.copy-btn').onclick=function(){var t=document.createElement('textarea');t.value=c;document.body.appendChild(t);t.select();document.execCommand('copy');document.body.removeChild(t);this.textContent='✓ Copied!';setTimeout(()=>this.textContent='Copy',2000);};
# MAGIC   });
# MAGIC })();
# MAGIC </script>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### Additional notes
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - Declarative Automation Bundles support substitutions and custom variables, making bundle configuration files more modular and reusable. Both allow dynamic retrieval of values, so settings can be determined when the bundle is deployed and run.
# MAGIC - To use a variable in a YAML file, reference it with a dollar sign, curly braces, and the mapping followed by the variable name.
# MAGIC - Common default substitutions include `${{bundle.target}}` for the target environment, `${{workspace.file_path}}` for the workspace file path, and `${{resources.jobs.<job-name>.id}}`, among others.
# MAGIC - Custom variables are declared under the top-level `variables` mapping. A simple custom variable is assumed to be of type string; for more structured data, set `type: complex`.
# MAGIC - The complex example `my_cluster` defines a default with a Spark version, node type, and number of workers.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Defining Custom Variables

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Creating and Referencing Simple Custom Variables
# MAGIC
# MAGIC The bundle settings file can contain one top-level `variables` mapping. Variables can reference other variables with `${var.name}` &mdash; here `catalog_dev` and `catalog_prod` build on `my_lab_user_name`.
# MAGIC
# MAGIC
# MAGIC <style>
# MAGIC .b1wrap { display:grid; grid-template-columns:minmax(240px, 0.9fr) minmax(0, 1.1fr); gap:18px; max-width:1150px; margin:0 auto; font-family:sans-serif; align-items:start; }
# MAGIC .b1left { display:flex; flex-direction:column; gap:10px; min-width:0; }
# MAGIC .b1card { background:#F9F7F4; border-radius:8px; padding:11px 14px; cursor:pointer; border-left:6px solid var(--cc); box-shadow:0 2px 8px rgba(27,49,57,0.06); transition:transform .1s; user-select:none; }
# MAGIC .b1card:hover { transform:translateX(3px); }
# MAGIC .b1card.active { background:#fff; outline:2px solid var(--cc); }
# MAGIC .b1lbl { font-weight:700; font-size:13.5pt; color:var(--cc); }
# MAGIC .b1sub { font-size:12.5pt; color:#0b2026; margin-top:3px; line-height:1.4; }
# MAGIC .b1right { min-width:0; }
# MAGIC .b1pre { background:#f8f8f8; border:1px solid #e0e0e0; border-radius:8px; padding:14px 16px; margin:0; overflow-x:auto; font-family:Consolas,Monaco,monospace; font-size:12pt; line-height:1.5; color:#1B3139; }
# MAGIC .b1ln { display:block; white-space:pre; padding:0 6px; border-radius:3px; transition:background .15s; }
# MAGIC .b1hint { font-size:12pt;color:#5E7077;font-style:italic;margin-bottom:8px; }
# MAGIC @media (max-width:780px){ .b1wrap{ grid-template-columns:1fr; } }
# MAGIC </style><div class="b1wrap"><div class="b1left"><div class="b1hint">Click an item to highlight it in the code &rarr;</div><div class="b1card" data-lines="1" data-idx="0" style="--cc:#98102A;" onclick="b1sel(0)"><div class="b1lbl">variables mapping</div><div class="b1sub">The bundle settings file can contain one top-level <b>variables</b> mapping where custom variables are defined.</div></div><div class="b1card" data-lines="2,3,4" data-idx="1" style="--cc:#4299E0;" onclick="b1sel(1)"><div class="b1lbl">Create a variable</div><div class="b1sub">Create a variable named <code>my_lab_user_name</code> with the default value of <i>labuser23904</i>.</div></div><div class="b1card" data-lines="6,7,8,10,11,12" data-idx="2" style="--cc:#00A972;" onclick="b1sel(2)"><div class="b1lbl">Reference a variable</div><div class="b1sub"><code>catalog_dev</code> and <code>catalog_prod</code> use the <code>my_lab_user_name</code> value and append strings.</div></div></div><div class="b1right"><div style="font-size:12pt;color:#5E7077;font-style:italic;margin-bottom:6px;">databricks.yml</div><pre class="b1pre"><span class="b1ln" data-ln="0">...</span><span class="b1ln" data-ln="1">variables:</span><span class="b1ln" data-ln="2">  my_lab_user_name:</span><span class="b1ln" data-ln="3">    description: Your user name</span><span class="b1ln" data-ln="4">    default: labuser23904</span><span class="b1ln" data-ln="5"> </span><span class="b1ln" data-ln="6">  catalog_dev:</span><span class="b1ln" data-ln="7">    description: Development catalog reference</span><span class="b1ln" data-ln="8">    default: ${var.my_lab_user_name}_1_dev</span><span class="b1ln" data-ln="9"> </span><span class="b1ln" data-ln="10">  catalog_prod:</span><span class="b1ln" data-ln="11">    description: Production catalog reference</span><span class="b1ln" data-ln="12">    default: ${var.my_lab_user_name}_3_prod</span><span class="b1ln" data-ln="13">...</span></pre></div></div>
# MAGIC <script>
# MAGIC (function(){
# MAGIC   var COLORS=['rgba(152,16,42,0.22)', 'rgba(66,153,224,0.22)', 'rgba(0,169,114,0.22)'];
# MAGIC   window.b1sel=function(idx){
# MAGIC     document.querySelectorAll('.b1ln').forEach(function(s){ s.style.background=''; });
# MAGIC     document.querySelectorAll('.b1card').forEach(function(c){ c.classList.remove('active'); });
# MAGIC     var card=document.querySelector('.b1card[data-idx="'+idx+'"]'); if(!card) return; card.classList.add('active');
# MAGIC     card.getAttribute('data-lines').split(',').forEach(function(n){ var s=document.querySelector('.b1ln[data-ln="'+n+'"]'); if(s) s.style.background=COLORS[idx]; });
# MAGIC   };
# MAGIC   window.addEventListener('load',function(){ window.b1sel(0); });
# MAGIC })();
# MAGIC </script>
# MAGIC
# MAGIC <div style="max-width:1150px;margin:14px auto 0 auto;font-family:sans-serif;display:flex;gap:14px;flex-wrap:wrap;">
# MAGIC   <div style="background:rgba(255,95,70,0.10);border-radius:6px;padding:10px 16px;font-family:Consolas,Monaco,monospace;font-size:13pt;color:#0b2026;"><b>catalog_dev</b> = labuser23904_1_dev</div>
# MAGIC   <div style="background:rgba(255,95,70,0.10);border-radius:6px;padding:10px 16px;font-family:Consolas,Monaco,monospace;font-size:13pt;color:#0b2026;"><b>catalog_prod</b> = labuser23904_3_prod</div>
# MAGIC </div>
# MAGIC
# MAGIC ##### Additional notes
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - Define the `variables` top-level mapping, then add custom variables beneath it. The bundle settings file can contain only one top-level `variables` mapping.
# MAGIC - Create a variable named `my_lab_user_name` with a default value of `labuser23904`.
# MAGIC - Reference a custom variable using `var` followed by a dot and the variable name in curly braces, then append literal text. Here `my_lab_user_name` is reused to build `catalog_dev` and `catalog_prod` by appending `_1_dev` and `_3_prod`.
# MAGIC - As a result, `catalog_dev` resolves to `labuser23904_1_dev` and `catalog_prod` resolves to `labuser23904_3_prod`.
# MAGIC - This technique lets you reference or override variables throughout the `databricks.yml` file.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Defining a Complex Variable
# MAGIC
# MAGIC A variable is assumed to be of type string unless you define it as a complex variable by setting `type: complex`. This lets a single variable carry structured data.
# MAGIC
# MAGIC
# MAGIC <style>
# MAGIC .b2wrap { display:grid; grid-template-columns:minmax(240px, 0.9fr) minmax(0, 1.1fr); gap:18px; max-width:1150px; margin:0 auto; font-family:sans-serif; align-items:start; }
# MAGIC .b2left { display:flex; flex-direction:column; gap:10px; min-width:0; }
# MAGIC .b2card { background:#F9F7F4; border-radius:8px; padding:11px 14px; cursor:pointer; border-left:6px solid var(--cc); box-shadow:0 2px 8px rgba(27,49,57,0.06); transition:transform .1s; user-select:none; }
# MAGIC .b2card:hover { transform:translateX(3px); }
# MAGIC .b2card.active { background:#fff; outline:2px solid var(--cc); }
# MAGIC .b2lbl { font-weight:700; font-size:13.5pt; color:var(--cc); }
# MAGIC .b2sub { font-size:12.5pt; color:#0b2026; margin-top:3px; line-height:1.4; }
# MAGIC .b2right { min-width:0; }
# MAGIC .b2pre { background:#f8f8f8; border:1px solid #e0e0e0; border-radius:8px; padding:14px 16px; margin:0; overflow-x:auto; font-family:Consolas,Monaco,monospace; font-size:12pt; line-height:1.5; color:#1B3139; }
# MAGIC .b2ln { display:block; white-space:pre; padding:0 6px; border-radius:3px; transition:background .15s; }
# MAGIC .b2hint { font-size:12pt;color:#5E7077;font-style:italic;margin-bottom:8px; }
# MAGIC @media (max-width:780px){ .b2wrap{ grid-template-columns:1fr; } }
# MAGIC </style><div class="b2wrap"><div class="b2left"><div class="b2hint">Click an item to highlight it in the code &rarr;</div><div class="b2card" data-lines="4,5,6,7,8,9,10" data-idx="0" style="--cc:#00A972;" onclick="b2sel(0)"><div class="b2lbl">type: complex</div><div class="b2sub">A variable is assumed to be of type <b>string</b> unless you define it as a <b>complex</b> variable.</div></div></div><div class="b2right"><div style="font-size:12pt;color:#5E7077;font-style:italic;margin-bottom:6px;">databricks.yml</div><pre class="b2pre"><span class="b2ln" data-ln="0">bundle:</span><span class="b2ln" data-ln="1">  name: demo08_bundle</span><span class="b2ln" data-ln="2">...</span><span class="b2ln" data-ln="3">variables:</span><span class="b2ln" data-ln="4">  my_cluster:</span><span class="b2ln" data-ln="5">    description: &quot;My cluster&quot;</span><span class="b2ln" data-ln="6">    type: complex</span><span class="b2ln" data-ln="7">    default:</span><span class="b2ln" data-ln="8">      spark_version: &quot;15.4.x-scala2.11&quot;</span><span class="b2ln" data-ln="9">      node_type_id: &quot;Standard_DS3_v2&quot;</span><span class="b2ln" data-ln="10">      num_workers: 2</span><span class="b2ln" data-ln="11">...</span></pre></div></div>
# MAGIC <script>
# MAGIC (function(){
# MAGIC   var COLORS=['rgba(0,169,114,0.22)'];
# MAGIC   window.b2sel=function(idx){
# MAGIC     document.querySelectorAll('.b2ln').forEach(function(s){ s.style.background=''; });
# MAGIC     document.querySelectorAll('.b2card').forEach(function(c){ c.classList.remove('active'); });
# MAGIC     var card=document.querySelector('.b2card[data-idx="'+idx+'"]'); if(!card) return; card.classList.add('active');
# MAGIC     card.getAttribute('data-lines').split(',').forEach(function(n){ var s=document.querySelector('.b2ln[data-ln="'+n+'"]'); if(s) s.style.background=COLORS[idx]; });
# MAGIC   };
# MAGIC   window.addEventListener('load',function(){ window.b2sel(0); });
# MAGIC })();
# MAGIC </script>
# MAGIC
# MAGIC ##### Additional notes
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - By default a variable is assumed to be of type string unless defined as a complex variable.
# MAGIC - The `my_cluster` variable is defined as `type: complex` with a default that specifies a Spark version, node type, and number of workers &mdash; providing structured data in one variable.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. Lookup Variables
# MAGIC
# MAGIC For specific object types you can define a **lookup** for a custom variable to retrieve the object's ID dynamically.
# MAGIC
# MAGIC
# MAGIC <style>
# MAGIC .b3wrap { display:grid; grid-template-columns:minmax(240px, 0.9fr) minmax(0, 1.1fr); gap:18px; max-width:1150px; margin:0 auto; font-family:sans-serif; align-items:start; }
# MAGIC .b3left { display:flex; flex-direction:column; gap:10px; min-width:0; }
# MAGIC .b3card { background:#F9F7F4; border-radius:8px; padding:11px 14px; cursor:pointer; border-left:6px solid var(--cc); box-shadow:0 2px 8px rgba(27,49,57,0.06); transition:transform .1s; user-select:none; }
# MAGIC .b3card:hover { transform:translateX(3px); }
# MAGIC .b3card.active { background:#fff; outline:2px solid var(--cc); }
# MAGIC .b3lbl { font-weight:700; font-size:13.5pt; color:var(--cc); }
# MAGIC .b3sub { font-size:12.5pt; color:#0b2026; margin-top:3px; line-height:1.4; }
# MAGIC .b3right { min-width:0; }
# MAGIC .b3pre { background:#f8f8f8; border:1px solid #e0e0e0; border-radius:8px; padding:14px 16px; margin:0; overflow-x:auto; font-family:Consolas,Monaco,monospace; font-size:12pt; line-height:1.5; color:#1B3139; }
# MAGIC .b3ln { display:block; white-space:pre; padding:0 6px; border-radius:3px; transition:background .15s; }
# MAGIC .b3hint { font-size:12pt;color:#5E7077;font-style:italic;margin-bottom:8px; }
# MAGIC @media (max-width:780px){ .b3wrap{ grid-template-columns:1fr; } }
# MAGIC </style><div class="b3wrap"><div class="b3left"><div class="b3hint">Click an item to highlight it in the code &rarr;</div><div class="b3card" data-lines="2,3" data-idx="0" style="--cc:#4299E0;" onclick="b3sel(0)"><div class="b3lbl">Create the new variable</div><div class="b3sub">Define a new custom variable &mdash; here <code>my_cluster_id</code>.</div></div><div class="b3card" data-lines="4,5" data-idx="1" style="--cc:#00A972;" onclick="b3sel(1)"><div class="b3lbl">Lookup the object</div><div class="b3sub">Use the <b>lookup</b> mapping to resolve the cluster name <code>myclustername</code> to its cluster ID.</div></div></div><div class="b3right"><div style="font-size:12pt;color:#5E7077;font-style:italic;margin-bottom:6px;">databricks.yml</div><pre class="b3pre"><span class="b3ln" data-ln="0">...</span><span class="b3ln" data-ln="1">variables:</span><span class="b3ln" data-ln="2">  my_cluster_id:</span><span class="b3ln" data-ln="3">    description: &quot;Get the cluster ID using a lookup variable&quot;</span><span class="b3ln" data-ln="4">    lookup:</span><span class="b3ln" data-ln="5">      cluster: myclustername</span><span class="b3ln" data-ln="6">...</span></pre></div></div>
# MAGIC <script>
# MAGIC (function(){
# MAGIC   var COLORS=['rgba(66,153,224,0.22)', 'rgba(0,169,114,0.22)'];
# MAGIC   window.b3sel=function(idx){
# MAGIC     document.querySelectorAll('.b3ln').forEach(function(s){ s.style.background=''; });
# MAGIC     document.querySelectorAll('.b3card').forEach(function(c){ c.classList.remove('active'); });
# MAGIC     var card=document.querySelector('.b3card[data-idx="'+idx+'"]'); if(!card) return; card.classList.add('active');
# MAGIC     card.getAttribute('data-lines').split(',').forEach(function(n){ var s=document.querySelector('.b3ln[data-ln="'+n+'"]'); if(s) s.style.background=COLORS[idx]; });
# MAGIC   };
# MAGIC   window.addEventListener('load',function(){ window.b3sel(0); });
# MAGIC })();
# MAGIC </script>
# MAGIC
# MAGIC ##### Additional notes
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - For specific object types, define a lookup for a custom variable to retrieve the object's ID.
# MAGIC - Specify the variable name (`my_cluster_id`), then use the `lookup` mapping to define what to look up &mdash; here the cluster named `myclustername`, whose ID becomes the variable's value.
# MAGIC - When a lookup is defined, the ID of the object with the specified name is always used as the variable's value, ensuring the correct resolved ID.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B4. Lookups for a Variety of Environment Configurations
# MAGIC
# MAGIC You can define lookups for many object types:
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:8px;max-width:1000px;margin:0 auto;font-family:sans-serif;"><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">alert</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">cluster_policy</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">cluster</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">dashboard</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">instance_pool</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">job</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">metastore</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">notification_destination</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">pipeline</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">query</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">service_principal</span><span style="background:rgba(255,95,70,0.08);border:1px solid #4299E0;border-radius:16px;padding:6px 16px;font-size:13.5pt;color:#0b2026;">warehouse</span></div>
# MAGIC
# MAGIC ##### Additional notes
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - Lookup variables are available for the alert, cluster_policy, cluster, dashboard, instance_pool, job, metastore, notification_destination, pipeline, query, service_principal, and warehouse object types.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Target Environment Overrides

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Target Environment Variable Overrides
# MAGIC
# MAGIC Use the `targets` mapping to dynamically modify a variable's value in each environment.
# MAGIC
# MAGIC
# MAGIC <style>
# MAGIC .c1wrap { display:grid; grid-template-columns:minmax(240px, 0.9fr) minmax(0, 1.1fr); gap:18px; max-width:1150px; margin:0 auto; font-family:sans-serif; align-items:start; }
# MAGIC .c1left { display:flex; flex-direction:column; gap:10px; min-width:0; }
# MAGIC .c1card { background:#F9F7F4; border-radius:8px; padding:11px 14px; cursor:pointer; border-left:6px solid var(--cc); box-shadow:0 2px 8px rgba(27,49,57,0.06); transition:transform .1s; user-select:none; }
# MAGIC .c1card:hover { transform:translateX(3px); }
# MAGIC .c1card.active { background:#fff; outline:2px solid var(--cc); }
# MAGIC .c1lbl { font-weight:700; font-size:13.5pt; color:var(--cc); }
# MAGIC .c1sub { font-size:12.5pt; color:#0b2026; margin-top:3px; line-height:1.4; }
# MAGIC .c1right { min-width:0; }
# MAGIC .c1pre { background:#f8f8f8; border:1px solid #e0e0e0; border-radius:8px; padding:14px 16px; margin:0; overflow-x:auto; font-family:Consolas,Monaco,monospace; font-size:12pt; line-height:1.5; color:#1B3139; }
# MAGIC .c1ln { display:block; white-space:pre; padding:0 6px; border-radius:3px; transition:background .15s; }
# MAGIC .c1hint { font-size:12pt;color:#5E7077;font-style:italic;margin-bottom:8px; }
# MAGIC @media (max-width:780px){ .c1wrap{ grid-template-columns:1fr; } }
# MAGIC </style><div class="c1wrap"><div class="c1left"><div class="c1hint">Click an item to highlight it in the code &rarr;</div><div class="c1card" data-lines="1" data-idx="0" style="--cc:#00A972;" onclick="c1sel(0)"><div class="c1lbl">targets mapping</div><div class="c1sub">Dynamically modify variable values within each target environment using the <b>target</b> top-level mapping.</div></div><div class="c1card" data-lines="2,3,4,5" data-idx="1" style="--cc:#4299E0;" onclick="c1sel(1)"><div class="c1lbl">development</div><div class="c1sub">When deploying to <b>development</b>, <code>target_catalog</code> uses the <code>catalog_dev</code> value.</div></div><div class="c1card" data-lines="6,7,8,9" data-idx="2" style="--cc:#98102A;" onclick="c1sel(2)"><div class="c1lbl">production</div><div class="c1sub">When deploying to <b>production</b>, <code>target_catalog</code> uses the <code>catalog_prod</code> value.</div></div></div><div class="c1right"><div style="font-size:12pt;color:#5E7077;font-style:italic;margin-bottom:6px;">databricks.yml</div><pre class="c1pre"><span class="c1ln" data-ln="0">...</span><span class="c1ln" data-ln="1">targets:</span><span class="c1ln" data-ln="2">  development:</span><span class="c1ln" data-ln="3">    ...</span><span class="c1ln" data-ln="4">    variables:</span><span class="c1ln" data-ln="5">      target_catalog: ${var.catalog_dev}</span><span class="c1ln" data-ln="6">  production:</span><span class="c1ln" data-ln="7">    ...</span><span class="c1ln" data-ln="8">    variables:</span><span class="c1ln" data-ln="9">      target_catalog: ${var.catalog_prod}</span></pre></div></div>
# MAGIC <script>
# MAGIC (function(){
# MAGIC   var COLORS=['rgba(0,169,114,0.22)', 'rgba(66,153,224,0.22)', 'rgba(152,16,42,0.22)'];
# MAGIC   window.c1sel=function(idx){
# MAGIC     document.querySelectorAll('.c1ln').forEach(function(s){ s.style.background=''; });
# MAGIC     document.querySelectorAll('.c1card').forEach(function(c){ c.classList.remove('active'); });
# MAGIC     var card=document.querySelector('.c1card[data-idx="'+idx+'"]'); if(!card) return; card.classList.add('active');
# MAGIC     card.getAttribute('data-lines').split(',').forEach(function(n){ var s=document.querySelector('.c1ln[data-ln="'+n+'"]'); if(s) s.style.background=COLORS[idx]; });
# MAGIC   };
# MAGIC   window.addEventListener('load',function(){ window.c1sel(0); });
# MAGIC })();
# MAGIC </script>
# MAGIC
# MAGIC <div style="max-width:1150px;margin:14px auto 0 auto;font-family:sans-serif;background:rgba(255,95,70,0.12);border-left:4px solid #FF5F46;border-radius:4px;padding:12px 16px;font-size:13.5pt;color:#0b2026;">
# MAGIC   A <b>default value</b> for the variable must be defined in the <b>variables</b> top-level mapping in order for an override to work.
# MAGIC </div>
# MAGIC
# MAGIC ##### Additional notes
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - Custom variables can be used throughout the `databricks.yml` file, including within the `targets` mapping, which lets you dynamically modify variable values for each target environment.
# MAGIC - When deploying to development, `target_catalog` uses the `catalog_dev` value; when deploying to production, it uses the `catalog_prod` value.
# MAGIC - If no override is specified for a variable, it defaults to its original value.
# MAGIC - A default value for the variable must be defined in the top-level `variables` mapping for an override to work.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Why Variables Matter

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. Benefits of Using Variables
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:24px;max-width:1150px;margin:0 auto;font-family:sans-serif;align-items:start;">
# MAGIC   <div style="text-align:center;padding:0 8px;">
# MAGIC   <div style="margin-bottom:12px;"><img src="data:image/png;base64,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" alt="Customizable for Different Environments" style="height:64px;vertical-align:middle;"/></div>
# MAGIC   <div style="font-size:15pt;font-weight:700;color:#0b2026;margin-bottom:8px;line-height:1.25;">Customizable for Different Environments</div>
# MAGIC   <div style="font-size:13pt;color:#0b2026;line-height:1.45;">Easily modify configurations (e.g., database connections, file paths, etc.) for development, staging, and/or production environments.</div>
# MAGIC </div>
# MAGIC   <div style="text-align:center;padding:0 8px;">
# MAGIC   <div style="margin-bottom:12px;"><img src="data:image/png;base64,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" alt="Reusability Across Databricks Projects" style="height:64px;vertical-align:middle;"/></div>
# MAGIC   <div style="font-size:15pt;font-weight:700;color:#0b2026;margin-bottom:8px;line-height:1.25;">Reusability Across Databricks Projects</div>
# MAGIC   <div style="font-size:13pt;color:#0b2026;line-height:1.45;">You can use the same asset bundle across multiple teams or workspaces by adjusting only variable values.</div>
# MAGIC </div>
# MAGIC   <div style="text-align:center;padding:0 8px;">
# MAGIC   <div style="margin-bottom:12px;"><img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAGAAAABgCAYAAADimHc4AAAAAXNSR0IArs4c6QAAAHhlWElmTU0AKgAAAAgABAEaAAUAAAABAAAAPgEbAAUAAAABAAAARgEoAAMAAAABAAIAAIdpAAQAAAABAAAATgAAAAAAAAJYAAAAAQAAAlgAAAABAAOgAQADAAAAAQABAACgAgAEAAAAAQAAAGCgAwAEAAAAAQAAAGAAAAAAtngHuQAAAAlwSFlzAABcRgAAXEYBFJRDQQAAHpZJREFUeAHtnQl4VNW9wM+5dxZiSIIKBCYDBoS6UCCLIQmgQq0o1qVora3PavVVX632KZ9brdaiVq1bW6niZ91Kte1rUVu1rVZrwUoWhCwg4EYpyiRsKkuAJDNz73m/M8nESXLvzGRD+jXHL947557t/v/nv//PRYjBMgiBQQgMQmAQAoMQGITAIAQGITAIgUEIDEJgEAKDEPhPgoA8qF921izP+KamzIg9JNO2okZ8rVIayiP9+w43m/bV1NRE4vX/jteDBgF506YdrsLmUcIQhVKJyQDzSBYXUEIeKoTK4ncHAvithJB7qdsppNwildqopFhLv7qIJd/etqZy+78LMj5TBAQLTphom9GTpS1OkVJNE9IYJQ0zBjulbKEsCziLJqH4EyKaAFQTDGSx+CzaQxBtuFG2BWrsj6WQq0DRKwz1yuZVlWsT+h10twccAcOPmpHlyxSnsWsvBLizDdPMAMDCtqxWIdV6dnGtLUUdO/t9j22GpCF2ej3hfU3NzWCjrVhZWcbQcDgzbHuGSSUDUMREoeRUrsW0mGSYnkz6M2ZUI+0Nhn/K8KvnQ1VVn7QPcdBcDhgCAsXFw6Xtv4g3v8wwjc/FABS1dgH0V9nmf5FCLQ8dGfyXWLKkA9C9gJIcXTh9LC9VbkjxJaXUHJAxUo9j29YHUsonRNR+PLS6qqEXYw9IlwFHQH7+rCHhw8OXGLa6Vno845RtaxZeA4N5UpjRFxpXrtw8IG/GoCM/X5rr8Zmn8ZKXwJZmStOErUW3MP/C/T570ScrVuwZqLnTHXdAEZA3tXS2MI27DMNTym4U8OhlShg/HW20vHSAtRcZKJjxRZSnq0DEl7TMYC1v2Ure3FhX8UK6wBqIdgOCgBGTZg31+1t/iAYz34DJI0zX2VLe3lhbsYSXgAQ+uxIsmHEabG8B1FCihTaLeVR6ojc2vPnmx5/FqvodAaMKS481pfkYcC+H3MMg4T6/bL1nY03N7nRfMK+w/CRDyjG2ZlcpiqF3s7K3heqqX0rRtONxsLw8Q7SIq1jbTYbHHMoGWauEdWlD7YrqjkYH6KZfEZA3tfx0YcjHeKlcFbXWweyvCNVVvd6Td5k0aZJvly97neHxTNA6aMoCa7OtyEf71JCJu+qX7UrZPqHB6KLSYkOYD0KlZVDDHoa6sqGu8qmEJgN+m8YbpreGYEHZpcIwfy4N6UfjWGIpzxVb697YkV7vT1sFJ5dPFl5Zg8xoUlI9K20U02RFitPRbkZDBrN7imw9rGaXPn/r/VKalyEYtIJwI+P8ONmU/fnM0x+D5RWUX83O/wkbVlpW9O7GulO/L8SC1PzDYXLlUTMN0+sV0cjLDbXVlzk06VTF3D9Fu7paRcKzeNAjatMD7Vi3TFvU/xMoKH+fzXM3ht1dgcJyX2Nd1W36+UCXBPO+d1MFCkq/I03jp7ALqVA1G+uqv+cGfG2EpZxFytlsQ70Tl6VsSwOoZGnMapZyFj+TUnTulCmZbm0a66vuY86LoYIwcuVWkHALbQe89AkB7JrzpelZGIOXsOfDP+93W3GwqOz6IZmyJlg0/dLi4mKvUzvNDgBpGVZx1DTEcqc2Xeu8hnelsCytzxeOKCnJ7fq8/bccU1j+da8n6828wrJ7XdqIhrqqX2GxXQIiQIJ5a4yy3Rr3U32vERAoKJsByT4C18EvY93UUFv1M7c1gahrhTAhbzlRGMYvtir/3/OKZ7DTO5chvv1HS2EG4f/vfiCnbOj81PnXBzVvbOVJPWpljj9qFnZtFZxSVhosKv+LMozfoP8fC4u5JlhQfk/XdvHfofrqX1u2fSVIUKz3Hij8rPizgbi2eb56OHJwankebOcFVM1cVM2HG2Jsx3kQDXxcD/dihqF024t4r2w0nCIs4guzR4/JHxYYs3Zkpn//kPzPnYYgvAUAjYMN/KGp9vm0DSTGGUe/E5ljQnbumN1Zebmbho0aPyI7ELwL2fQgVHoUquaHzPkkFDZZesxZ2SPzMvdsDeEG6V72bg3VMqZks3wBrnZS1oixLzRt2zwgdkIvKGABmpvgpczxdjS6TA6R13R/hbaaT4EvLDyel6NdfFeGxQw7av0IZt2En+ab6PrVzVkjVhrSfAEgzkYd3A9f/7PbmE71tlKvwLb2aosb4P5e2r5VtrDfBPBXoOujlFkPeAxPWUN91Xwcfd8AEfuFx7wuWDD9bqfxdF2ods5tIO0ZNtkIadqPx2wHt8Z9qO8xBQQLwpcC/Otsy95mesyzNq+odFQ1OwHfUpeH6isf1evcsyPU3LR189JhI8e+aAtrOLu+BArJhTrWQ/b3K9u4urG+skcGUdPW0OacQHAJRvZGNMlRrG8yqmk2gP4z7oaLGusqn9i95UOt7Yi9W0Lv5IwMvo24PgM3yayhUAL9HShhmTpk5NjX8al+mY1SwKaJNG0Jva7H6M+SVGvoOtGogrJ80zBW8nLDhYp+I1S74umubfTvDuCjnrDzvx0HvlPbvCKMNyGHK9n6+8aamv1ObXpSp51/kUPD5wpDtTTUVIEU5zK6qHyeIeTTyIVDRNS6N1Rfdb1Ty0Bh2RfZJFjZKqwMNbNxVVWdU7ve1vUIAXkFZU8ZXt8FViTyB9S2c5i0m5HUE+D3dtH91W/01PJ5pimfhtcnRQKa0yLD470cKni1oS5vrhB9cpl3Wn7aLAjAToe074dV7FWW8TWE0kedRuJHB/BFO89vZztd2x0sv/duC72TDTsinHkG8mfW0FzY0fHlfxfr13faWBnD81YZUnwdragge+Tu+j306693SBsB2aPGsAs8Rws7+rOG1VW/67qAvKKyr6I7/wI+HkHswfOrHuva5mD8rYGZlRtYRzj0DBOZkP3xro/RjlYkrnXf9oZ9OaPHRNmAc+Gq48cenrN4x44dfQkcdQyflhakdz8Cay5azw7T8D7Q0TvhxlJqLepjm8eTcGLCo4P+FiVgo14kwG1BaK9yWvB+n/VLOxLdCKWU7PRmn+bUpjd1aSEAjeGKmF9fiScxfLY4TbS1bsV6TJf72CVe6PdeLQyd2h18dQsMvN73gARSX9TjyLZKpzW2R8+0s1FHU6+kTY/kp9OYui4lArTmw5Rn4vLdbxgqpkq6DdbssxYSCH8Xta0kcmjLxW7t0q3PLygYljelNOjUPlBSMkbHmZ2e9aQuWPjSPHw/p2JQbvWanjuS9bVky6/hAtvBwIl5BdNJAuh7SYkAfDLz4P1D0a9fCdVWJXUPtO0SdYt2joHbm8cRk+3NEkcXlR2TVzj9PsvMrMOS/bbTGDLq+aYhMurziqY/FCguK3Jqk6pOOwcJkd5OXovmP3e6UXd8nK11ddrmeY4N5iGJ4Px4fV+uqRBgELM7G96u6e036UzUUHvqM1i3fyUoE2j1mril0ydVDZBg4fQfo5+vhCVcgzMmn96dNJKONeB7ZSfmsXu/Yyijug0RPaMI/yHqKoB5DFS70rNzSFLqjs9rSOM3WOtwW3VWf7DZpAjAiTUe7aAEU36H8Fl/jy8i+ZU4gLLvV5ZGmrpoZGlpLC0keR8hggWlE/2Z6hVcCTfgY/agc29kV6bopqNh1vv4l1pxGYAI/9LA1PJuDjmnQdpc0+pb+I/Ao1y4adOyFqd2XevMT7wrcVG8D9VMjB7e0mc2lBQBxKJOYCf7uS7vSdCaxc3GWcfa5cvb5851dFUkvljguBmkJJovAUTtil4rDftEAucPxTPeEtsm3mvWAWU+gdkxk128AgXg8wjJl8YUTC9JbOd0v23Nmv10/mM7+5nt1MapLoYoabyGnwkvhY5d9K0kRQCc/ATNQXjP19KdJlA8azhb6gJ2Cd3FA2JB8shYcFL5YdJSvwN4R2pLk1y2k0I11SswjtKK1jGJr6HuzdXNXnsOAvLFmIdWqt+5Ce+E91AqajwM4pqpOyd3yrRxCc+S3yq1VFMn9Al8+lbcEaCDJkoUox1oR/LKJNMYsKoJBFougA8/Ie3WKvjyGKJjaybm+JL1iw2pvOIuqGwqgKiVYfW13ibWagUgHPafDxKXSo93HP7/hUzg/n48bFxT8S7s63V2c47HNJfmFc5YHCiafnGguPxoQWa22ztHrWgNLlbNsqZOKC3NdmuXTr3rJMGobyTOp3EgeoeMim7aT95UAiqmfQa5mcfTZjI7zw8v1fmYiAD7fcOQC5ctWxZNtohA8cyZqB+XADQ8leYloXXL+5S7qeO7Y0tmfouAWjVCfF6gqOxLjbXVLyZbAzh62Fa23v1HsREuZD0Xqmg0krc7vF4Wlb8hbOOlUH3FyzzXFB0r2+z9oTyVvYkfn2vZbxzB9a3Yg178z3WHkIyQj9WXCRVsDK3rntQqpf1jw/DOR008jmjwTgD/IrbCfEPJElu2FmyurXwy1XqkHb0aA8+DKH2ooW756lTt03n+4crluKTVPVp+sDmIVRC/SFJ0Zpw3y1ugTFGkrMh3eY/nYC3b2UxTpeG5Uklr0YQJczuHUNetC4Oo91i6YXnNCUmGT/nIlQLg/UfEhKBtb3QaBR4d0bwJXniZ8ItnG3qYeczuH4tj7xReuAld/GGnOXpbJ1usJy0prgGIM/IK/za5oU4kRe6mZTENSLuZ9d+D+QWzhkXN8EmwGXxehJAci7EBWYfX20pfdjiMk2x35GkBjEq42aFfRxUO/5repH0byj4BtjWUCf6xpa7yg44B4zdoh223rnlBbFTn0vDOmx+T/v6yNpiEjH7BuZV77SYSvJTpW8EYHWynW2ul2uFiA6felyQIUDEzH6U+qRrJAJ3JM821AL3j9A7iFZc5dQH6kbZ6dYjTc1hjhq7XW8TxuZRL9SPYUEqV1Km/aUd9TvXxOlgWp3D01ASn+lBcEYA+PSw2rhR9Eoyua1NqfMzQMtS7Tm0MW65DmPN+8pSu8dg2C9Q4Q29QYrxrnfqj3fxT90dBGOv0vK91ZKDtZg6GaYdTLwd0RQC2duwZVkD7TuzlDO7dMvULmMpoc2F3aRdWWcsx+d9CSH9etYrFY4qnH6nzRjmAcUT0sDDJv4ZWXddHW/2vduka+6kM8UkMgYbIEeeeazq16Usd4Glt648i3YfiioBPx0TcDkiRsYAGe9TxBbateWUfCLqc4P8n8PJz8S+t3uXLeotc6NUYbf9l2+oj5NOl7amF3VaIjPFpFgeXCItJk5zZVLdePahgh7a1dpVRaQ3mjoC4E6xjorTGS7+REtu0bx0eEXTr1FhfXSGlNRtj8Dn0DbQROTEG06j1vLDVyW6+ez0e7QMYhFpGbE9ljbvNfyDqk6ihqo01GPqYaP8XbAfS18GylMcx+mK3Gci8WMOzc/JLSkbpQ3mGbezZXFfR6NY+Xg8CirUPRSnr7XhdT65RYUQM0S9Rx6TTuiIA+fYJCVjYf8mlPCdfXPTkpPMStTcqTE6osENPmjBhgn/Dhg3tPNW536aVK3UKov5Lp3AkTZ1qIGM4qvqPdDoktkHoHyZa7UlsDnZI/3OvxLlcEQDxtoUepTuLiA2kZDku58btK1ZsSxw41b3pt1epFvUe1uYxzdkjThZiw59S9Un3eV7RDM4ci1JcEtuk13ojnX5auHukOh14ny5aRQEa1igEPbai5U/eX1ujvS/uMkBZH2gtgh06zml4RI+HhCW8XfIhb8RYhzPur3nF5TcGjiubMb64OMepT2Idxlsz4uWxNjkgb9IaTuLzPtyza60faBcHIuapVG70sZNnjif6tghirxemR+eRngoShjEIarDSPqDncnK2dzfI4oYiQaEjiotH93a9rgiIKrUJBLRCgOPb0sY7T4ED61rbjt5BmwoYuR+/0Rx8Q3catrk8LIasASGXde7R/ZdP+B/FhbxBxwF2+bOu696i5zVk2l0GVZ1mRyPbYOGuGduMLANF5VdYXruakOvlWGyZOAVfQPX9hsfyHptrhAtDtRVzyfqe73SiEw2tmbR4G3dNUVT5q7U3uOer1VLQpeTPIsVvd+taVLl8IyoKNq9xP/Kv/TrkAmmynw1lnEgKyyQ0l9UTc4Ycl8ojSnCbY6NCZ0JrUr4kVFv5tMuSUlYHC8vmKmkugXNnEo+4uKG++pdOnQ7DhZwRMXXA5wLN49npzyjDvquxprrWqb1jHbZF8P3Nx5NVdyPCfo7Wd2FXC0cbkWudEOY4BpWuCNAdSMl7lpS8s9kZF6Z7eE07siKytR75NQZkzN5cV5lSCMICbiaCdjtvgNFngQTnnFO3l9D1uJ7PIF77NBGubNb7M50J7dQ+5mgzwjoANIfoG9+VEFf3BenMYUBJ38FgvRu2dwhj/i6jKeeiDRteSqpUxNfmyoJiDaQ+paJxpE6Kd0h11Y4seizmBQ0cBf+bjhUKcn+kovYdUBtGmecxp8MbyebVcWBDGL9iJ2az8x8E+Lihuxfcyv6IaH1KA592G5S056QCfu6UOZm6X/fROmrsxtqqB6GkM6GAj2Cn5zUP3aWDQUk3d7x3UgQoSy7D3Mfdok4gB8fZKRYfKeGK1Vqhg/KUL4788MO0nFUN9ZU3wwvuhS34YWcPpnWejAn0cSdY2M8B6jCAupgzCFdR3V1oUrk/a9cC+P3pUEgDp0XOdGM5YyeXIJjL70SeVHs8e9c3Z+3WMu0PYwqnf00IZ7cGx5teM5Q4Bxmym8SCyzRVMGXKkhQBjdE968DsOpTpcRx6KE85mm4AbwSI17cF5cXjqKd4DdMrnk/8t4DwWjSRY4dkWOel02u75T8NBWAGwN9oRk3NdhyBHywsPxG2eK1WLKDoi7bUVjsaaJwLO9fyehHM3hthZ6W8P1kdagII/rIyjd/mFTY855YQ1sZurdjxJrTDO3V+U6p3SIoAoSM/SuqTK1xUWgAJvh86j/Ynod2ELOH9MQtI25LRGQcA6ad60RhB7LbUZIwv6XwQrmXpQx++tXyn7tu1aBWXRdwFe/Cg3/9E79aubfTvGJIM+SsAP4Lo3nNshjnKMI/CsTcJm+K7UHUjYcszpeV70u2goZZfbIZfs4my8egucJonsS45AnRLGV3CQqJSGfNyp0xPmuOjBRwgu037awhm3Nqbg9rCUktRb5sYYSpf0ToscbFd7/Pz84cwXzHr49NOltbZHctOX9apbIpyBORGvxG+26lRDElKEMo0h6BePoT6eU5j/YpXG2uWf9hYU/WO5vOWHdGZFyGQcPpW2683iGNBnfsB7XZi48xLlbWXEgGkfKxhe73OpMO9hvq644ztlVHZOp9D00fyolW5ZmRxsrZuzzyH+vVhOIJAKjvaIpJmHMihudrgG45O/nGLynD1DxGUuVRTCbS4yO2bFbt92eWwzWkgc0PEzrrBaX1bV69cx95aoAmT66W0cYTf1vrqTaQnIOw9XsOSlziNFa9zHCD+sP3KOSPxELoybEFcOXyG82HrYFHpFNjGfMgvDJ+9vie6cJf52n8CNrKBnZ+11Ua9HgxY5YVttfptn2PcQifxAq1ZsJQm5fH83m08JjpBs1rGe167wt3a8U06kg+iu2DJBUcUH5/r1s6Q6lcIZAuYne5kyMb7pYMAcVjrnj+jYq3Cup+Q0WxfGO+ceLWVeSyAyAIJCGFzWuKzAb5nZ6BQWmE2Zfcio95S1j0UGVHZuPIfm7u3aKth3ePb7ozVbm10/baz53AySH0AFWRZKjzCre0IGV2DzHgbeXKEzxee7NYuLQSsQxhrQ0NHsDjAcJ2TFsBJxP8jeHIdxhcIkPdzqKNfXAtuC0+3Huzglo6xjIpkfZBZ7QiMX5O1bnsGPByRrp9qDqAMWYX2pBWEQrfR0kKA7tyQ4/sjJLUMWXAEWsD3nQbU31vABogBHk8ip8w/eyQAoQkaAvD/d5zW/Gmd3BC7l+7A0s/zlyxDEZH5IGyPLyKTusexn9brPuRQHamvTiVtBAiy3Pjk1/cgq1Z483e019NpwBgSoAT9LB0k4AsqcBqnH+sO1cF78oSS2iPENV5HfsHaxVnJjMCIv/UsjLkcULrqX2uT2zjgvc1FrwxX7TF9BAARnTTLDl+ItuCXlnzYze2cDhICx5UXYmm+iBW7YnTRtOP7EeCdh1Jo8RT8NNrZ51oCsqUa5WElmss4fwbfinAoOpGY6ms1KwZRj3OfVEmg0V5NffxH/pNz6REC9BDhZuN2tIBadsHkVssbM5qchu5AAktMpARtS+jTL3yIqYLvAp3O411EFVyFmdPYPaqTbXFFt+B/fCzNs/kG7A+glhawdRNniE+JP9NXnRqjfGoRn0OYAKW8lhPZ80zic6d7bCfsIpigIXc5Pdd1PUbAR+9WNBED+haL2M1uuZjjqY5eRz14okxAMN/DQe8nPYaqQo5oZ5kXn/0j6Jolodrq53T7gSjs1e3aBvBEo3mpxo/YZh1UQM6nkUlysdbzO4odFkeg0Zyn2Rkb/y9aMel46HID7EfHHvFNO5cmPUeAHqhxdVUdlPVdTYEEv+9mR3/FbYI4JWh1Af/KN9EK9Ec+/oZdcSIugW9rS9Otb3/UMy/CV5+lMF1VQTx6Xh1A8ggOeRjmFJAQQRgsTZx/iAhvgZWsiO1oadzHOz+jbZ/ENl3vQRXPoXFpvNv1Wfx3jykg3jEWH7Dt2xjcy/s9yXd1To4/63rVSMBHdgMvtp4/YgtVp1BX2bXdQPxGE+E70vo4mdJyptv7apU6YPtegO08op2OtH3NUuIkXA8PJa5HW9D6EAhuihup/wgWfA5R2Qq+rHVJYrv4vWZZZAaUs9lIn43Uxeu7XrstqGuDZL9x/f5Q+03IvxmKkFuio1tu7WEz99iypaQ9sOPosXTr25d65bXfhF3qdPPisYXHH911LLy8PyKH91Q2xgcIzAsaaivnbKmteKNrO/1bHwLRH/Tjg07TiPg9CnIz8XouctLk+OZWGZHBcXR7zzss01UF7hMC9KJC9cGrMMAehs/moNEsgTS/oeudSn98DcVp3GR17UH5l5FXPltZFyW2JSB/KL/nYd+0cK7hK8SAf83vlJtD+3oI5FyGd+ZxfF9+tJ2zE8fV98Q08D+Z4Eg+157+3rVJ7HefEaC/HNJQN+dKHQaEHWUgeBa3ffDOOXDhuIoBrkRePcKOtVFI/1unn8Sna5VhnYB8qLJUY3Y4sCZen/5V/b1dEx2b2Ed/jxSWNg9tkY9S6UOE7qUfEKAHX2DHvkalrOvxqdimad4aLAotiR3CcJ/7gD1B3lShu/8Rn9DhOAbu7DKx1iTUvn07egELrIGuhbNlsCUd2RsCcp7YXFP5z65NEn/3YtLE7p3vEVz3spsIy9lbIPl5nKV+IxAL43Vu9xn8UkSzvg8V7GJnns+nyjqpmL1fT3dfUGBXy03YCrMRvh9EwvZdqcbuVwToyfjc2PNg/gQiSC8jnMeiT/+WU5TP5hXOnJpqMQP5vHFVxbtxPxW60AP6M8vS9O/tlzlRsfQ4WiNi599CWmQUqrpy+9rU2YL9jgC9EP1NicZs/xn4ja5C398hTe/ZfFO3CkQ8GDuUrRt9BoVs68cQuD9BOGZgnf7WVBFNCdG+L8VWmqrQtB7hj3xG9T0iammlWg4IAmIvhPMuVFe5kDO1OkcTlU16QMQV0rJryDhYrOOvIslZ3L4DxXmEhgljrocVPchOHcrfHQAsA53FSvdTBYmjIjx08AmCl2cj4H/BWB423fdRte9PbJfsXhuoB6SMKZxxHEdfr2KycxCGGQBBO6lqcUU8b0j7ZVuF18bVVJx0G3ipoKXso7XK57ZAxgzg53wP7euThto5+VoZ0LHdnZ7sSTgM55LmMhSjz9F1zkdBboBN3IqF5kf/34OpfM1+f/T37d8Fcpuyo15/EUCq1pvxBFwVs46VvQfWM59vTifVejoGaL85YAiITxz79wUEKYFSfhVqPZJdSC4tyFDqfTSoN+GmdcS3roVihlm2OiY1AmzMfNmCLn4H16n46afBkI/F7cHX1SIRYdnjG9asCMXnT7zGqFCKu/n4b6n2mSEj0FjUn1jbq9FIdL2V5f3oI6+3WfeZ0Nx8SEuryKXtFDbOyVQhQ8w82I1e+6uw2hu0iyZx/HTuDzgC4ovSPvchGeJ4SPdMXnoWb8FJdQ+P0Qn1P0dF/g4fhUlJASRYvcdHXzPb85BiyGSsf4HM10HmnyLW0JeTxXjz+bJX+LDw1+HFlzN5Cdob3fGd6n/VCZcDyNzFMy1ltdE2Qqe2QHGxLwJQ+TrzLGqordCeUXhRz8tnhoDEpcZcvc3yKN6rhN1UzKKOASnDbcMzN5mzLr9k1qhItPUV4MNhb7GefrWE8Ve1RHxvu50dS5y30z3yCBWyFIqcS305f/zTWOpwrofwx/BiH+PvgDpgeaqSeMhLm+srV7U/49K7clAgwGnpOhWysWY8uzDpNzoNvvuTwadn9jmN0Zc6TaH+YdEcOyL1PxZnE85psoyWXXE51ZexB/sOQmAQAoMQGITAIAQGITAIgUEIDEJgEAKDEBiEwCAE/nMh8P+wwhg7Mnf9pwAAAABJRU5ErkJggg==" alt="Easy Maintenance &amp; Updates" style="height:64px;vertical-align:middle;"/></div>
# MAGIC   <div style="font-size:15pt;font-weight:700;color:#0b2026;margin-bottom:8px;line-height:1.25;">Easy Maintenance &amp; Updates</div>
# MAGIC   <div style="font-size:13pt;color:#0b2026;line-height:1.45;">Quickly update assets by modifying variables to ensure consistency and reduce errors.</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### Additional notes
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - Variables let you customize configurations &mdash; catalogs, file paths, and other settings &mdash; for development, staging, and production environments.
# MAGIC - They enable reusability across Databricks projects: the same asset bundle can be used across multiple teams or workspaces by adjusting only the variable values.
# MAGIC - They make maintenance and updates easy: assets and configurations can be updated quickly, keeping environments consistent and reducing the risk of errors.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Conclusion
# MAGIC
# MAGIC - Bundles provide many **default substitutions** you can reference with `${...}`.
# MAGIC - You can define **simple**, **complex**, and **lookup** custom variables in the `variables` mapping.
# MAGIC - **Target overrides** give a variable a different value per environment (a default is required).
# MAGIC - Variables make bundles **reusable, maintainable, and customizable** across environments.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next demo, you will deploy a DAB to multiple environments using variables and overrides.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
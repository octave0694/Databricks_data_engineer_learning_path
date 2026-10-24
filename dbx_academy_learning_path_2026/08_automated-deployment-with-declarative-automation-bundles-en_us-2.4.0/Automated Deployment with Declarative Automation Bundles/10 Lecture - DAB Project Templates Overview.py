# Databricks notebook source
# MAGIC %md
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - DAB Project Templates Overview
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this short lecture, you will learn that you do not have to build a bundle from scratch. Databricks provides default bundle templates to scaffold a project, and you can also create your own custom templates.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC 1. **Use** a Databricks default bundle template to create a bundle.
# MAGIC 2. **Describe** what makes up a custom bundle template.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Bundle Project Templates

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Default Bundle Templates
# MAGIC
# MAGIC Use a Databricks default bundle template to scaffold your bundle with `databricks bundle init`.
# MAGIC
# MAGIC <div style="font-family:sans-serif;max-width:1000px;margin:0 auto 14px auto;">
# MAGIC <div style="display:flex;flex-wrap:wrap;gap:8px;max-width:1000px;margin:0 auto;font-family:sans-serif;"><span style="background:rgba(255,95,70,0.08);border:1px solid #FF5F46;border-radius:16px;padding:6px 16px;font-size:14pt;color:#0b2026;">default-python</span><span style="background:rgba(255,95,70,0.08);border:1px solid #FF5F46;border-radius:16px;padding:6px 16px;font-size:14pt;color:#0b2026;">default-sql</span><span style="background:rgba(255,95,70,0.08);border:1px solid #FF5F46;border-radius:16px;padding:6px 16px;font-size:14pt;color:#0b2026;">dbt-sql</span><span style="background:rgba(255,95,70,0.08);border:1px solid #FF5F46;border-radius:16px;padding:6px 16px;font-size:14pt;color:#0b2026;">mlops-stacks</span></div>
# MAGIC </div>
# MAGIC <div class="code-block" data-language="bash">
# MAGIC databricks bundle init default-python
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
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC _Speaker notes:_
# MAGIC
# MAGIC - Now, let's take a quick look at Declarative Automation Bundles templates.
# MAGIC - You have two main options here: you can either use the pre-configured templates provided by Databricks, or you can create your own custom templates to meet your specific needs.
# MAGIC - Databricks offers several default bundle templates, including:
# MAGIC - default-python for Python-based projects
# MAGIC - default-sql for SQL-based projects
# MAGIC - dbt-sql for projects involving DBT (Data Build Tool) workflows
# MAGIC - mlops-stacks for machine learning operations stacks
# MAGIC - To get started with one of these templates, you can use the Databricks CLI command databricks bundle init <template_name>. This command will automatically initialize your asset bundle based on the selected template, saving you time in setting up the configuration from scratch.
# MAGIC - One note, when using a bundle template within a Databricks notebooks, you get the entire template. If you are running the CLI command outside of a notebook, a series of prompts enable you to select specific portions of the template.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Custom Bundle Templates
# MAGIC
# MAGIC You can also build your own template and initialize from a local path or remote URL.
# MAGIC <div class="rc2-w">
# MAGIC <style>
# MAGIC .rc2-w { width:100%; display:flex; flex-direction:column; align-items:center; gap:18px; font-family:sans-serif; }
# MAGIC .rc2-w * { box-sizing:border-box; }
# MAGIC .rc2-row { display:block; white-space:nowrap; font-size:0; width:100%; max-width:1100px; }
# MAGIC .rc2-box { display:inline-block; width:32%; margin-right:2%; min-height:88px; background:#F9F7F4; border-top:8px solid transparent; border-left:2px solid transparent; border-right:2px solid transparent; border-bottom:2px solid transparent; border-radius:8px; padding:16px 14px; text-align:center; cursor:pointer; user-select:none; vertical-align:top; transition:transform .12s; box-shadow:0 2px 8px rgba(27,49,57,0.06); white-space:normal; }
# MAGIC .rc2-box:last-child { margin-right:0; }
# MAGIC .rc2-box:hover { transform:translateY(-2px); }
# MAGIC .rc2-box.active { background:#fff; border-left-color:var(--pc); border-right-color:var(--pc); border-bottom-color:var(--pc); }
# MAGIC .rc2-l { display:block; font-weight:700; font-size:14pt; letter-spacing:2px; text-transform:uppercase; line-height:1.2; pointer-events:none; }
# MAGIC .rc2-t { display:block; font-size:14pt; color:#0b2026; line-height:1.3; margin-top:6px; pointer-events:none; }
# MAGIC .rc2-dw { overflow:hidden; max-height:0; opacity:0; transition:max-height .35s ease, opacity .28s ease, margin-top .28s ease; margin-top:0; width:100%; max-width:1100px; }
# MAGIC .rc2-dw.open { max-height:900px; opacity:1; margin-top:12px; }
# MAGIC .rc2-dc { background:#F9F7F4; border-radius:10px; padding:22px 26px; border-top:7px solid #ccc; box-shadow:0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .rc2-dt { font-size:17pt; font-weight:700; color:#0b2026; margin-bottom:12px; } .rc2-db { font-size:14pt; color:#0b2026; line-height:1.55; } .rc2-db ul { margin:0; padding-left:22px; } .rc2-db li { margin-bottom:8px; }
# MAGIC </style>
# MAGIC <div style="width:100%;max-width:1100px;text-align:center;font-size:14pt;color:#5E7077;font-style:italic;">For more detail, <strong style="color:#FF5F46;">click each card below</strong>.</div>
# MAGIC <div class="rc2-row"><span class="rc2-box" data-id="0" onclick="rc2S(0)" style="--pc:#4299E0;border-top-color:#4299E0;"><span class="rc2-l" style="color:#4299E0;">Requirements</span><span class="rc2-t">Minimum files</span></span><span class="rc2-box" data-id="1" onclick="rc2S(1)" style="--pc:#00A972;border-top-color:#00A972;"><span class="rc2-l" style="color:#00A972;">Customization</span><span class="rc2-t">Make it your own</span></span><span class="rc2-box" data-id="2" onclick="rc2S(2)" style="--pc:#FFAB00;border-top-color:#FFAB00;"><span class="rc2-l" style="color:#FFAB00;">Usage</span><span class="rc2-t">Init from path/URL</span></span></div>
# MAGIC <div class="rc2-dw" id="rc2-dw"><div class="rc2-dc" id="rc2-dc"></div></div>
# MAGIC </div>
# MAGIC <script>
# MAGIC var rc2D=[{title:"Minimum files",color:"#4299E0",text:"<ul><li>At a minimum, must have <code>databricks_template_schema.json</code> and <code>databricks.yml.tmpl</code>.</li></ul>"},
# MAGIC {title:"Make it your own",color:"#00A972",text:"<ul><li>You can add user prompts, a specific folder structure, and more.</li></ul>"},
# MAGIC {title:"Init from path/URL",color:"#FFAB00",text:"<ul><li>Pass its local path or remote URL to the CLI <code>bundle init</code> command.</li></ul>"}]; var rc2C=null;
# MAGIC function rc2S(id){var w=document.getElementById('rc2-dw'),c=document.getElementById('rc2-dc'),d=rc2D[id];
# MAGIC document.querySelectorAll('.rc2-box').forEach(function(b){b.classList.toggle('active',parseInt(b.dataset.id)===id&&rc2C!==id);});
# MAGIC if(rc2C===id){w.classList.remove('open');rc2C=null;return;} rc2C=id; c.style.borderTopColor=d.color;
# MAGIC c.innerHTML='<div class="rc2-dt" style="color:'+d.color+';">'+d.title+'</div><div class="rc2-db">'+d.text+'</div>'; w.classList.add('open');}
# MAGIC </script>
# MAGIC
# MAGIC <div class="code-block" data-language="bash">
# MAGIC databricks bundle init /projects/templates/test-template
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
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC _Speaker notes:_
# MAGIC
# MAGIC - In addition to  Databricks' default templates, you can create custom bundle templates specific to your organization.
# MAGIC - Although we won’t go into the details of creating custom templates in this course, here are a few key points:
# MAGIC - Custom templates require at least databricks_template_schema.json and databricks.yml.tmpl.
# MAGIC - You can include user prompts, define folder structures, and customize settings.
# MAGIC - To use a custom template, simply pass the local path or remote URL of the template to the Databricks CLI bundle init command.
# MAGIC - Custom templates help organizations create and manage bundles in a consistent and repeatable way, establishing folder structures, tasks, and DevOps infrastructure-as-code (IaC) for development and deployment.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Conclusion
# MAGIC
# MAGIC - Databricks provides **default bundle templates** (`default-python`, `default-sql`, `dbt-sql`, `mlops-stacks`) to scaffold a bundle with `databricks bundle init`.
# MAGIC - You can also build **custom templates** (minimum: a schema JSON and a `databricks.yml.tmpl`) and initialize from a local path or URL.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next lab, you will use a Databricks default DAB template to create a bundle.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC
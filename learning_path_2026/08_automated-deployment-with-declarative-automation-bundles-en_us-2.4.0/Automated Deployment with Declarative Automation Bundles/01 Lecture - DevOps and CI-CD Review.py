# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - DevOps and CI/CD Review
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will review the DevOps and CI/CD foundations that the rest of this course builds on. These topics are prerequisites, so the review moves quickly and establishes a shared vocabulary before you begin deploying Databricks projects with Declarative Automation Bundles (DABs).
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC 1. **Describe** the core concepts and benefits of DevOps and explain how they extend to data engineering as DataOps.
# MAGIC 2. **Explain** Continuous Integration and Continuous Delivery/Deployment (CI/CD), and distinguish continuous delivery from continuous deployment.
# MAGIC 3. **Describe** the role of automated testing across unit, integration, and system tests.
# MAGIC 4. **Explain** how environments and data are isolated across DEV, STAGE, and PROD.
# MAGIC 5. **Identify** the tools available to deploy Databricks projects: the REST API, the Databricks SDKs, and the Databricks CLI.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. DevOps Review

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. What is DevOps?
# MAGIC
# MAGIC <div class="devops-intro-section">
# MAGIC <style>
# MAGIC .devops-intro-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .devops-intro-section *{box-sizing:border-box;}
# MAGIC .devops-main-grid{display:grid;grid-template-columns:.95fr 1.05fr;gap:22px;align-items:stretch;}
# MAGIC .devops-venn-card,.devops-benefits-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:22px;}
# MAGIC .devops-venn-wrap{position:relative;min-height:285px;display:flex;align-items:center;justify-content:center;}
# MAGIC .devops-circle{width:230px;height:230px;border-radius:50%;display:flex;align-items:center;justify-content:center;text-align:center;padding:24px;font-size:18px;line-height:1.25;font-weight:800;}
# MAGIC .devops-circle.left{background:#FFF1EE;border:2px solid #FF5F46;color:#0B2026;margin-right:0px;}
# MAGIC .devops-circle.right{background:#EAF7F2;border:2px solid #00A972;color:#0B2026;margin-left:0px;}
# MAGIC .devops-center{position:absolute;left:50%;top:28%;transform:translate(-50%,-50%);width:128px;height:128px;border-radius:50%;background:#FF5F46;color:#fff;display:flex;align-items:center;justify-content:center;text-align:center;font-size:22px;font-weight:900;box-shadow:0 8px 20px rgba(27,49,57,.12);}
# MAGIC .devops-callout{margin-top:18px;background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:16px 18px;font-size:17px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC .devops-benefits-title{font-size:20px;font-weight:900;margin:0 0 16px 0;color:#0B2026;text-align:center;}
# MAGIC .devops-benefit-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;}
# MAGIC .devops-benefit{background:#F9F7F4;border:1px solid #DCE0E2;border-top:5px solid #FF5F46;border-radius:12px;padding:18px;min-height:106px;display:flex;align-items:center;justify-content:center;text-align:center;font-size:18px;line-height:1.3;font-weight:800;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .devops-benefit:nth-child(2){border-top-color:#2272B4;}
# MAGIC .devops-benefit:nth-child(3){border-top-color:#00A972;}
# MAGIC .devops-benefit:nth-child(4){border-top-color:#98102A;}
# MAGIC @media screen and (max-width:900px){.devops-main-grid,.devops-benefit-grid{grid-template-columns:1fr;}.devops-venn-wrap{min-height:320px;}.devops-circle{width:210px;height:210px;}}
# MAGIC </style>
# MAGIC <div class="devops-main-grid">
# MAGIC <div class="devops-venn-card">
# MAGIC <div class="devops-venn-wrap">
# MAGIC <div class="devops-circle left">Software Engineering Best Practices</div>
# MAGIC <div class="devops-circle right">IT Operations</div>
# MAGIC <div class="devops-center">DevOps</div>
# MAGIC </div>
# MAGIC <div class="devops-callout">Fosters <strong>collaboration</strong> between development and operations teams to <strong>automate Jobs</strong> and <strong>streamline processes</strong>.</div>
# MAGIC </div>
# MAGIC <div class="devops-benefits-card">
# MAGIC <div class="devops-benefits-title">Key benefits include</div>
# MAGIC <div class="devops-benefit-grid">
# MAGIC <div class="devops-benefit">Faster deployments</div>
# MAGIC <div class="devops-benefit">Improved collaboration</div>
# MAGIC <div class="devops-benefit">Enhanced reliability</div>
# MAGIC <div class="devops-benefit">Better scalability</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC #### What is DevOps?
# MAGIC DevOps is the union of two worlds that traditionally worked separately.
# MAGIC
# MAGIC - **Software engineering best practices** contribute version control, automated testing, and repeatable builds.
# MAGIC - **IT operations** contributes deployment, monitoring, and infrastructure management.
# MAGIC
# MAGIC The overlap in the middle is DevOps. A culture and a set of practices that get those two groups working together instead of throwing work over a wall.
# MAGIC
# MAGIC #### Why it matters
# MAGIC - **Faster deployments.** Automation removes slow, manual handoffs, so changes reach users sooner.
# MAGIC - **Improved collaboration.** Dev and ops share ownership of the whole lifecycle rather than blaming each other when something breaks.
# MAGIC - **Enhanced reliability.** Automated testing and consistent deployments mean fewer surprises in production.
# MAGIC - **Better scalability.** Repeatable, automated processes scale far better than manual ones.
# MAGIC
# MAGIC In short, DevOps is a way to build and deliver software quickly and reliably by bridging the gap between development and operations..
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. The DevOps Lifecycle
# MAGIC
# MAGIC DevOps is a process for continuously integrating, testing, and deploying your code.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### DataOps = DevOps for Data Engineering
# MAGIC <div class="dataops-close-section">
# MAGIC <style>
# MAGIC .dataops-close-section{witext-align:center;font-size:30px;line-height:1.15;font-weight:900;}
# MAGIC .dataops-text{background:#fff;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:22px;font-size:22px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC .dataops-text strong{color:#FF5F46;}
# MAGIC @media screen and (max-width:800px){.dataops-hero{grid-template-columns:1fr;}.dataops-text{font-size:18px;}}
# MAGIC </style>
# MAGIC <div class="dataops-hero">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/lecture_devops_review/dataops_devops.png" alt="DataOps DevOps for Data Engineering">
# MAGIC <div class="dataops-text">You want to think about how <strong>DevOps</strong> principles and culture can be applied to your <strong>Data Engineering pipelines</strong>.</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>DataOps is essentially DevOps for data engineering—it applies those same principles of automation, collaboration, and continuous improvement to data Lakeflow Jobs. Just as DevOps streamlines software delivery, DataOps focuses on optimizing the flow, quality, and management of data pipelines.</p>
# MAGIC <p>In the end, you want to think about how DevOps principles and culture can be applied to your Data Engineering pipelines.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A3. DevOps for Data Engineering and Machine Learning
# MAGIC
# MAGIC The same DevOps principles extend to data pipelines as DataOps, and to machine learning as MLOps.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-family:sans-serif;max-width:1100px;margin:0 auto 8px auto;color:#5E7077;font-size:13pt;font-style:italic;"></div>
# MAGIC <style>
# MAGIC .ds14 { max-width:900px; margin:0 auto; font-family:sans-serif; display:flex; flex-direction:column; align-items:center; }
# MAGIC .ds14-top { background:#FF5F46; color:#fff; border-radius:8px; padding:16px 40px; text-align:center; box-shadow:0 2px 8px rgba(27,49,57,0.20); }
# MAGIC .ds14-top .h { font-size:18pt; font-weight:700; }
# MAGIC .ds14-tag { font-size:14pt; color:#0b2026; text-align:center; font-style:italic; margin:14px 0; max-width:560px; }
# MAGIC .ds14-conn { width:2px; height:18px; background:#618794; }
# MAGIC .ds14-branch { display:flex; gap:60px; justify-content:center; position:relative; }
# MAGIC .ds14-branch::before { content:""; position:absolute; top:-18px; left:25%; right:25%; height:2px; background:#618794; }
# MAGIC .ds14-child { background:#F9F7F4; border-radius:8px; box-shadow:0 2px 8px rgba(27,49,57,0.06); padding:20px 26px; text-align:center; width:280px; position:relative; overflow:hidden; }
# MAGIC .ds14-child::before { content:""; position:absolute; top:0; left:0; width:100%; height:8px; }
# MAGIC .ds14-child.a::before { background:#00A972; } .ds14-child.b::before { background:#4299E0; }
# MAGIC .ds14-child .h { font-size:17pt; font-weight:700; color:#0b2026; margin-bottom:4px; } .ds14-child .d { font-size:14pt; color:#618794; }
# MAGIC </style>
# MAGIC <div class="ds14">
# MAGIC   <div class="ds14-top"><div class="h">DevOps</div></div>
# MAGIC   <div class="ds14-tag">Apply DevOps principles to Data Engineering and Machine Learning</div>
# MAGIC   <div class="ds14-conn"></div>
# MAGIC   <div class="ds14-branch">
# MAGIC     <div class="ds14-child a"><div class="h">DataOps</div><div class="d">Data Engineering</div></div>
# MAGIC     <div class="ds14-child b"><div class="h">MLOps</div><div class="d">Machine Learning</div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC - We can apply DevOps principles to Data Engineering and Machine Learning. Remember, DevOps is all about automating processes, improving collaboration, testing, and speeding up delivery.
# MAGIC - DataOps is a subset of DevOps and applies DevOps to data engineering. It automates the management of data pipelines, ensuring smooth, reliable data flows from collection to processing. This means fewer bottlenecks and faster insights.
# MAGIC - MLOps is about applying DevOps to machine learning. It streamlines the process of deploying and managing ML models, ensuring that models move from development to production quickly and are monitored for performance.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A4. DataOps = DevOps for Data Engineering
# MAGIC
# MAGIC DataOps applies the same principles of automation, collaboration, and continuous improvement to your data workflows.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### DataOps = DevOps for Data Engineering
# MAGIC <div class="dataops-close-section">
# MAGIC <style>
# MAGIC .dataops-close-section{witext-align:center;font-size:30px;line-height:1.15;font-weight:900;}
# MAGIC .dataops-text{background:#fff;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:22px;font-size:22px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC .dataops-text strong{color:#FF5F46;}
# MAGIC @media screen and (max-width:800px){.dataops-hero{grid-template-columns:1fr;}.dataops-text{font-size:18px;}}
# MAGIC </style>
# MAGIC <div class="dataops-hero">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/lecture_devops_review/dataops_devops.png" alt="DataOps DevOps for Data Engineering">
# MAGIC <div class="dataops-text">You want to think about how <strong>DevOps</strong> principles and culture can be applied to your <strong>Data Engineering pipelines</strong>.</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - DataOps is essentially DevOps for data engineering—it applies those same principles of automation, collaboration, and continuous improvement to data workflows. Just as DevOps streamlines software delivery, DataOps focuses on optimizing the flow, quality, and management of data pipelines.
# MAGIC - In the end, you want to think about how DevOps principles and culture can be applied to your Data Engineering pipelines.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Continuous Integration and Continuous Deployment/Delivery (CI/CD) Review

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. The Role of CI/CD in DevOps
# MAGIC
# MAGIC CI/CD is a key subset of DevOps that automates code integration, testing, and delivery. It splits into two complementary halves.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img src="https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/lecture_devops_review/ci-cd_role.png" alt="Role of CI/CD" style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC - CI/CD is a key subset of DevOps practices that focuses on automating code integration, testing, and delivery. Within the DevOps lifecycle, continuous integration (CI) emphasizes planning, development, environment management, and testing of the pipelines.
# MAGIC - On the other hand, continuous deployment/delivery (CD) focuses on automating release processes, deployment, operation, and monitoring of these pipelines.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Continuous Integration (CI) High-Level Overview

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="cicd-ci-overview">
# MAGIC <style>
# MAGIC .cicd-ci-overview{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cicd-ci-overview *{box-sizing:border-box;}
# MAGIC .ci-callout{background:#ffffff;border:1.5px solid #0B2026;border-radius:2px;padding:18px 22px;text-align:center;font-size:22px;line-height:1.35;margin-bottom:20px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .ci-benefit-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;align-items:stretch;}
# MAGIC .ci-benefit{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:16px;min-height:120px;box-shadow:0 2px 8px rgba(27,49,57,.06);display:flex;align-items:center;justify-content:center;text-align:center;}
# MAGIC .ci-benefit:nth-child(2){border-left-color:#00A972;}
# MAGIC .ci-benefit:nth-child(3){border-left-color:#2272B4;}
# MAGIC .ci-benefit:nth-child(4){border-left-color:#98102A;}
# MAGIC .ci-benefit h3{font-size:17px;line-height:1.3;margin:0;color:#0B2026;font-weight:850;}
# MAGIC .ci-placeholder{margin-top:18px;border:2px dashed #DCE0E2;border-radius:14px;background:#F9F7F4;min-height:180px;display:flex;align-items:center;justify-content:center;text-align:center;color:#5A6F77;font-size:18px;font-weight:800;}
# MAGIC @media screen and (max-width:900px){.ci-benefit-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="ci-callout">CI involves regularly merging code changes from multiple contributors into a central repository and running automated tests to ensure code quality.</div>
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img src="https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/lecture_devops_review/ci_high_level_overview.png" alt="CI High Level Overview" style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-family:sans-serif;max-width:1100px;margin:0 auto 8px auto;color:#5E7077;font-size:13pt;font-style:italic;"></div><div class="rc3-w">
# MAGIC <style>
# MAGIC .rc3-w { width:100%; display:flex; flex-direction:column; align-items:center; gap:18px; font-family:sans-serif; }
# MAGIC .rc3-w * { box-sizing:border-box; }
# MAGIC .rc3-row { display:block; white-space:nowrap; font-size:0; width:100%; max-width:1100px; }
# MAGIC .rc3-box { display:inline-block; width:23.5%; margin-right:2%; min-height:88px; background:#F9F7F4; border-top:8px solid transparent; border-left:2px solid transparent; border-right:2px solid transparent; border-bottom:2px solid transparent; border-radius:8px; padding:16px 14px; text-align:center; cursor:pointer; user-select:none; vertical-align:top; transition:transform .12s; box-shadow:0 2px 8px rgba(27,49,57,0.06); white-space:normal; }
# MAGIC .rc3-box:last-child { margin-right:0; }
# MAGIC .rc3-box:hover { transform:translateY(-2px); }
# MAGIC .rc3-box.active { background:#fff; border-left-color:var(--pc); border-right-color:var(--pc); border-bottom-color:var(--pc); }
# MAGIC .rc3-l { display:block; font-weight:700; font-size:14pt; letter-spacing:2px; text-transform:uppercase; line-height:1.2; pointer-events:none; }
# MAGIC .rc3-t { display:block; font-size:14pt; color:#0b2026; line-height:1.3; margin-top:6px; pointer-events:none; }
# MAGIC .rc3-dw { overflow:hidden; max-height:0; opacity:0; transition:max-height .35s ease, opacity .28s ease, margin-top .28s ease; margin-top:0; width:100%; max-width:1100px; }
# MAGIC .rc3-dw.open { max-height:700px; opacity:1; margin-top:12px; }
# MAGIC .rc3-dc { background:#F9F7F4; border-radius:10px; padding:22px 26px; border-top:7px solid #ccc; box-shadow:0 2px 8px rgba(27,49,57,0.06); }
# MAGIC .rc3-dt { font-size:17pt; font-weight:700; color:#0b2026; margin-bottom:12px; } .rc3-db { font-size:14pt; color:#0b2026; line-height:1.55; } .rc3-db ul { margin:0; padding-left:22px; } .rc3-db li { margin-bottom:8px; }
# MAGIC </style>
# MAGIC <div style="width:100%;max-width:1100px;text-align:center;font-size:14pt;color:#5E7077;font-style:italic;">For more detail, <strong style="color:#FF5F46;">click each card below</strong>.</div>
# MAGIC <div class="rc3-row"><span class="rc3-box" data-id="0" onclick="rc3S(0)" style="--pc:#4299E0;border-top-color:#4299E0;"><span class="rc3-l" style="color:#4299E0;">Early Detection</span><span class="rc3-t">Catch issues sooner</span></span><span class="rc3-box" data-id="1" onclick="rc3S(1)" style="--pc:#00A972;border-top-color:#00A972;"><span class="rc3-l" style="color:#00A972;">Faster Cycle</span><span class="rc3-t">Ship features faster</span></span><span class="rc3-box" data-id="2" onclick="rc3S(2)" style="--pc:#FFAB00;border-top-color:#FFAB00;"><span class="rc3-l" style="color:#FFAB00;">Collaboration</span><span class="rc3-t">Cleaner code</span></span><span class="rc3-box" data-id="3" onclick="rc3S(3)" style="--pc:#98102A;border-top-color:#98102A;"><span class="rc3-l" style="color:#98102A;">Automated Testing</span><span class="rc3-t">Stable code</span></span></div>
# MAGIC <div class="rc3-dw" id="rc3-dw"><div class="rc3-dc" id="rc3-dc"></div></div>
# MAGIC </div>
# MAGIC <script>
# MAGIC var rc3D=[{title:"Catch issues sooner",color:"#4299E0",text:"<ul><li>Integrating code often means bugs and conflicts are caught early, making them easier to fix.</li></ul>"},
# MAGIC {title:"Ship features faster",color:"#00A972",text:"<ul><li>Frequent integration speeds up the delivery of new features and fixes.</li></ul>"},
# MAGIC {title:"Cleaner code",color:"#FFAB00",text:"<ul><li>Regular integration leads to cleaner, more modular code and better teamwork.</li></ul>"},
# MAGIC {title:"Stable code",color:"#98102A",text:"<ul><li>Automated tests run with each integration to ensure code is stable and works with existing features.</li></ul>"}]; var rc3C=null;
# MAGIC function rc3S(id){var w=document.getElementById('rc3-dw'),c=document.getElementById('rc3-dc'),d=rc3D[id];
# MAGIC document.querySelectorAll('.rc3-box').forEach(function(b){b.classList.toggle('active',parseInt(b.dataset.id)===id&&rc3C!==id);});
# MAGIC if(rc3C===id){w.classList.remove('open');rc3C=null;return;} rc3C=id; c.style.borderTopColor=d.color;
# MAGIC c.innerHTML='<div class="rc3-dt" style="color:'+d.color+';">'+d.title+'</div><div class="rc3-db">'+d.text+'</div>'; w.classList.add('open');}
# MAGIC </script>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - CI involves regularly merging code changes from multiple contributors into a central repository and running automated tests to ensure code quality. Tests that fail do not make it into your source code.
# MAGIC - For example, imagine three developers working on a project, implementing updates or fixes.
# MAGIC - The developers push their code to a version control system, which automatically runs tests before the commit is made. This ensures that any issues with the code are caught. If the tests fail, the commit is stopped. If the tests pass, the changes are implemented.
# MAGIC - The way testing and commits to version control are handled depends on the branching strategy defined by your organization. Deciding on the right branching and testing strategy is crucial for maintaining quality and smooth workflows.
# MAGIC - Here are four key benefits of Continuous Integration:
# MAGIC - First, Early Detection of Issues—by integrating code often, bugs and conflicts are caught early, making them easier to fix.
# MAGIC - Second, Faster Development Cycle—frequent integration speeds up the delivery of new features and fixes.
# MAGIC - Third, Improved Collaboration and Code Quality—regular integration leads to cleaner, more modular code and better teamwork.
# MAGIC - Finally, Automated Testing and Validation—automated tests run with each integration to ensure the code is stable and works with existing features.
# MAGIC - These benefits—early issue detection, faster delivery, better collaboration, and automated testing—make Continuous Integration essential for smooth development.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. Review High-Level Testing Steps
# MAGIC
# MAGIC Automated testing follows the **testing pyramid**: fast, cheap tests at the base run constantly; slower, broader tests at the top run less often.
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:minmax(0,0.9fr) minmax(0,1.1fr);gap:24px;max-width:1150px;margin:0 auto;font-family:sans-serif;align-items:center;">
# MAGIC   <div style="text-align:center;"><svg viewBox="0 0 380 320" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:auto;max-width:380px;font-family:Helvetica,Arial,sans-serif;">
# MAGIC     <defs><marker id="l1pa" markerWidth="8" markerHeight="8" refX="4" refY="3" orient="auto"><path d="M0,0 L6,3 L0,6 Z" fill="#9AA7AE"/></marker>
# MAGIC           <marker id="l1pas" markerWidth="8" markerHeight="8" refX="4" refY="3" orient="auto"><path d="M6,0 L0,3 L6,6 Z" fill="#9AA7AE"/></marker></defs>
# MAGIC     <polygon points="190,20 360,300 20,300" fill="#3B82C4"/>
# MAGIC     <line x1="105" y1="205" x2="275" y2="205" stroke="#fff" stroke-width="2"/>
# MAGIC     <line x1="148" y1="130" x2="232" y2="130" stroke="#fff" stroke-width="2"/>
# MAGIC     <text x="190" y="120" font-family="Helvetica,Arial,sans-serif" font-size="13" font-weight="700" fill="#fff" text-anchor="middle">System</text><text x="190" y="137" font-family="Helvetica,Arial,sans-serif" font-size="13" font-weight="700" fill="#fff" text-anchor="middle">Tests</text>
# MAGIC     <text x="190" y="178" font-family="Helvetica,Arial,sans-serif" font-size="13" font-weight="700" fill="#fff" text-anchor="middle">Integration</text><text x="190" y="195" font-family="Helvetica,Arial,sans-serif" font-size="13" font-weight="700" fill="#fff" text-anchor="middle">Tests</text>
# MAGIC     <text x="190" y="265" font-family="Helvetica,Arial,sans-serif" font-size="13" font-weight="700" fill="#fff" text-anchor="middle">Unit Tests</text>
# MAGIC     <line x1="8" y1="40" x2="8" y2="300" stroke="#9AA7AE" stroke-width="2" marker-start="url(#l1pas)" marker-end="url(#l1pa)"/>
# MAGIC     <text x="2" y="32" font-family="Helvetica,Arial,sans-serif" font-size="12" font-weight="700" fill="#0b2026" text-anchor="start">Slow</text>
# MAGIC     <text x="2" y="300" font-family="Helvetica,Arial,sans-serif" font-size="12" font-weight="700" fill="#0b2026" text-anchor="start">Fast</text>
# MAGIC   </svg></div>
# MAGIC   <div style="display:flex;flex-direction:column;gap:12px;">
# MAGIC     <div style="border:1px solid #3B82C4;border-radius:6px;padding:12px 16px;font-size:13pt;color:#0b2026;"><b>System Tests</b> &mdash; Test the entire application, ensuring that all parts function together in a real-world scenario. <br><b>Ex:</b> End to end data pipeline in a Workflow.</div>
# MAGIC     <div style="border:1px solid #3B82C4;border-radius:6px;padding:12px 16px;font-size:13pt;color:#0b2026;"><b>Integration Tests</b> &mdash; Test the interaction between different components or systems. <br><b>Ex:</b> Notebooks / Declarative Pipelines / Jobs.</div>
# MAGIC     <div style="border:1px solid #3B82C4;border-radius:6px;padding:12px 16px;font-size:13pt;color:#0b2026;"><b>Unit Tests</b> &mdash; Test <b>individual</b> functions or methods in isolation. Fast, low cost, high coverage, and automated. <br><b>Ex:</b> Custom pyspark functions.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - The testing pyramid categorizes tests into unit, integration, and system tests. Testing your code is essential.
# MAGIC - Unit tests form the base: they test individual functions or methods in isolation, run quickly and automatically, and provide the broadest coverage at low cost &mdash; for example, testing a custom pyspark function.
# MAGIC - Integration tests sit in the middle: they test the interaction between components or systems, and are slower and costlier than unit tests &mdash; for example, notebooks, declarative pipelines, or jobs.
# MAGIC - System tests sit at the top: they test the entire application end to end in a production-like environment, and are the slowest and most expensive &mdash; for example, an end-to-end data pipeline in a Workflow.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B4. Continuous Delivery/Deployment (CD) Overview
# MAGIC
# MAGIC Both share the same automated pipeline up to staging. **Continuous Delivery** keeps a manual gate before production; **Continuous Deployment** automates that final step too.
# MAGIC
# MAGIC <div style="max-width:1120px;margin:0 auto;font-family:sans-serif;">
# MAGIC   <div style="margin-bottom:22px;">
# MAGIC     <div style="font-size:14.5pt;font-weight:700;color:#1B5162;margin-bottom:6px;">Continuous Delivery (CD) &mdash; manual push to production</div>
# MAGIC     <div style="font-size:12.5pt;color:#0b2026;margin-bottom:10px;">Automatically pushes changes to staging/pre-production, <b>with the ability to manually deploy to production</b> at any time.</div>
# MAGIC     <div style="display:flex;align-items:flex-start;"><div style="flex:1;text-align:center;"><div style="height:20px;"></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:600;line-height:1.2;background:#C7CDD1;color:#5A6F77;">Develop</div></div><div style="flex:0 0 auto;padding-top:20px;"><div style="height:54px;display:flex;align-items:center;color:#3B82C4;font-size:14pt;padding:0 3px;">&#8594;</div></div><div style="flex:1;text-align:center;"><div style="height:20px;"></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:600;line-height:1.2;background:#C7CDD1;color:#5A6F77;">Version<br>Control</div></div><div style="flex:0 0 auto;padding-top:20px;"><div style="height:54px;display:flex;align-items:center;color:#3B82C4;font-size:14pt;padding:0 3px;">&#8594;</div></div><div style="flex:1;text-align:center;"><div style="height:20px;"><div style="font-size:9.5pt;font-weight:700;color:#fff;background:#0b2026;border-radius:3px;padding:1px 6px;display:inline-block;">AUTO</div></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:600;line-height:1.2;background:#C7CDD1;color:#5A6F77;">Build</div></div><div style="flex:0 0 auto;padding-top:20px;"><div style="height:54px;display:flex;align-items:center;color:#3B82C4;font-size:14pt;padding:0 3px;">&#8594;</div></div><div style="flex:1;text-align:center;"><div style="height:20px;"><div style="font-size:9.5pt;font-weight:700;color:#fff;background:#0b2026;border-radius:3px;padding:1px 6px;display:inline-block;">AUTO</div></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:600;line-height:1.2;background:#C7CDD1;color:#5A6F77;">Test</div></div><div style="flex:0 0 auto;padding-top:20px;"><div style="height:54px;display:flex;align-items:center;color:#3B82C4;font-size:14pt;padding:0 3px;">&#8594;</div></div><div style="flex:1;text-align:center;"><div style="height:20px;"><div style="font-size:9.5pt;font-weight:700;color:#fff;background:#0b2026;border-radius:3px;padding:1px 6px;display:inline-block;">AUTO</div></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:600;line-height:1.2;background:#C7CDD1;color:#5A6F77;">Deploy to<br>Stage</div></div><div style="flex:0 0 auto;padding-top:20px;"><div style="height:54px;display:flex;align-items:center;color:#3B82C4;font-size:14pt;padding:0 3px;">&#8594;</div></div><div style="flex:1;text-align:center;"><div style="height:20px;"><div style="font-size:9.5pt;font-weight:700;color:#fff;background:#F0A500;border-radius:3px;padding:1px 6px;display:inline-block;">MANUAL</div></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:700;line-height:1.2;background:#0b2026;color:#fff;">Deploy to<br>Production</div></div></div>
# MAGIC   </div>
# MAGIC   <div>
# MAGIC     <div style="font-size:14.5pt;font-weight:700;color:#1B5162;margin-bottom:6px;">Continuous Deployment (CD) &mdash; automatic push to production</div>
# MAGIC     <div style="font-size:12.5pt;color:#0b2026;margin-bottom:10px;">A <b>fully automated process</b> where each change that passes tests is immediately deployed to <b>production</b>.</div>
# MAGIC     <div style="display:flex;align-items:flex-start;"><div style="flex:1;text-align:center;"><div style="height:20px;"></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:600;line-height:1.2;background:#C7CDD1;color:#5A6F77;">Develop</div></div><div style="flex:0 0 auto;padding-top:20px;"><div style="height:54px;display:flex;align-items:center;color:#3B82C4;font-size:14pt;padding:0 3px;">&#8594;</div></div><div style="flex:1;text-align:center;"><div style="height:20px;"></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:600;line-height:1.2;background:#C7CDD1;color:#5A6F77;">Version<br>Control</div></div><div style="flex:0 0 auto;padding-top:20px;"><div style="height:54px;display:flex;align-items:center;color:#3B82C4;font-size:14pt;padding:0 3px;">&#8594;</div></div><div style="flex:1;text-align:center;"><div style="height:20px;"><div style="font-size:9.5pt;font-weight:700;color:#fff;background:#0b2026;border-radius:3px;padding:1px 6px;display:inline-block;">AUTO</div></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:600;line-height:1.2;background:#C7CDD1;color:#5A6F77;">Build</div></div><div style="flex:0 0 auto;padding-top:20px;"><div style="height:54px;display:flex;align-items:center;color:#3B82C4;font-size:14pt;padding:0 3px;">&#8594;</div></div><div style="flex:1;text-align:center;"><div style="height:20px;"><div style="font-size:9.5pt;font-weight:700;color:#fff;background:#0b2026;border-radius:3px;padding:1px 6px;display:inline-block;">AUTO</div></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:600;line-height:1.2;background:#C7CDD1;color:#5A6F77;">Test</div></div><div style="flex:0 0 auto;padding-top:20px;"><div style="height:54px;display:flex;align-items:center;color:#3B82C4;font-size:14pt;padding:0 3px;">&#8594;</div></div><div style="flex:1;text-align:center;"><div style="height:20px;"><div style="font-size:9.5pt;font-weight:700;color:#fff;background:#0b2026;border-radius:3px;padding:1px 6px;display:inline-block;">AUTO</div></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:600;line-height:1.2;background:#C7CDD1;color:#5A6F77;">Deploy to<br>Stage</div></div><div style="flex:0 0 auto;padding-top:20px;"><div style="height:54px;display:flex;align-items:center;color:#3B82C4;font-size:14pt;padding:0 3px;">&#8594;</div></div><div style="flex:1;text-align:center;"><div style="height:20px;"><div style="font-size:9.5pt;font-weight:700;color:#fff;background:#0b2026;border-radius:3px;padding:1px 6px;display:inline-block;">AUTO</div></div><div style="height:54px;display:flex;align-items:center;justify-content:center;text-align:center;border-radius:8px;padding:0 6px;font-size:11.5pt;font-weight:700;line-height:1.2;background:#FF5F46;color:#fff;">Deploy to<br>Production</div></div></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - Continuous Delivery automates pushing changes to staging/pre-production environments, with the ability to manually deploy to production whenever needed &mdash; giving controlled releases. For example, after CI and testing, you decide when to deploy the pipeline to production.
# MAGIC - Continuous Deployment takes automation a step further: once a change passes all tests, it is automatically deployed to production, delivering features and fixes quickly without manual intervention.
# MAGIC - Continuous Deployment requires well-designed tests to confidently decide the pipeline should be deployed.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B5. High-Level CI/CD Workflow Overview
# MAGIC
# MAGIC The full workflow combines **Continuous Integration** (develop, build, test, version control) with **Continuous Delivery/Deployment** (deploy to stage, deploy to production).
# MAGIC
# MAGIC <div style="max-width:1120px;margin:0 auto;font-family:sans-serif;">
# MAGIC   <div style="display:flex;align-items:center;justify-content:center;flex-wrap:wrap;gap:6px 0;"><div style="height:6px;background:#1E7FD1;width:14px;flex:0 0 auto;"></div><div style="background:#0b2026;color:#fff;border-radius:8px;padding:14px 14px;text-align:center;font-size:12.5pt;min-width:72px;">Develop</div><div style="height:6px;background:#1E7FD1;width:14px;flex:0 0 auto;"></div><div style="background:#0b2026;color:#fff;border-radius:8px;padding:14px 14px;text-align:center;font-size:12.5pt;min-width:72px;">Build</div><div style="height:6px;background:#1E7FD1;width:14px;flex:0 0 auto;"></div><div style="background:#0b2026;color:#fff;border-radius:8px;padding:14px 14px;text-align:center;font-size:12.5pt;min-width:72px;">Test</div><div style="height:6px;background:#1E7FD1;width:14px;flex:0 0 auto;"></div><div style="background:#0b2026;color:#fff;border-radius:8px;padding:14px 14px;text-align:center;font-size:12.5pt;min-width:72px;">Version<br>Control</div><div style="width:2px;background:#0b2026;align-self:stretch;margin:0 12px;flex:0 0 auto;"></div><div style="background:#1E7FD1;color:#fff;border-radius:8px;padding:14px 14px;text-align:center;font-size:12.5pt;min-width:72px;">Deploy to<br>Stage</div><div style="height:6px;background:#1E7FD1;width:14px;flex:0 0 auto;"></div><div style="background:#1E7FD1;color:#fff;border-radius:8px;padding:14px 14px;text-align:center;font-size:12.5pt;min-width:72px;">Deploy to<br>Production</div><div style="height:6px;background:#1E7FD1;width:14px;flex:0 0 auto;"></div><div style="width:0;height:0;border-top:11px solid transparent;border-bottom:11px solid transparent;border-left:15px solid #1E7FD1;flex:0 0 auto;"></div></div>
# MAGIC   <div style="display:flex;justify-content:space-between;max-width:1000px;margin:16px auto 0 auto;font-size:13.5pt;color:#0b2026;">
# MAGIC     <div style="flex:1;text-align:center;">Continuous Integration (CI)</div>
# MAGIC     <div style="flex:1;text-align:center;">Continuous Delivery / Continuous Deployment (CD)</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - The high-level CI/CD workflow combines continuous integration (develop, build, test, and version control) with continuous delivery/deployment (deploying the project to stage and production), automating the entire process for fast, error-free development.
# MAGIC - CI/CD streamlines development of data pipelines by automating testing and deployment, leading to faster, more reliable releases.
# MAGIC - This approach minimizes manual errors, improves collaboration, and ensures high-quality software delivered quickly and consistently.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B6. Isolating Environments for CI/CD
# MAGIC
# MAGIC A CI/CD workflow keeps development and testing away from live production. In Databricks you isolate environments using multiple **workspaces**, multiple **catalogs**, or both.
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr;gap:26px;max-width:1000px;margin:0 auto;font-family:sans-serif;">
# MAGIC   <div style="border:1.5px dashed #8A9BA8;border-radius:8px;padding:20px;text-align:center;">
# MAGIC     <div style="font-size:16pt;font-weight:700;color:#0b2026;margin-bottom:6px;">Workspaces</div>
# MAGIC     <div style="font-size:13pt;color:#0b2026;margin-bottom:14px;">Utilizing multiple workspaces, one for each environment.</div>
# MAGIC     <div style="background:#1B5162;color:#fff;border-radius:4px;padding:9px;text-align:center;font-weight:700;font-size:13pt;margin-bottom:8px;">DEV</div><div style="background:#1B5162;color:#fff;border-radius:4px;padding:9px;text-align:center;font-weight:700;font-size:13pt;margin-bottom:8px;">STAGE</div><div style="background:#1B5162;color:#fff;border-radius:4px;padding:9px;text-align:center;font-weight:700;font-size:13pt;margin-bottom:8px;">PROD</div>
# MAGIC   </div>
# MAGIC   <div style="border:1.5px dashed #8A9BA8;border-radius:8px;padding:20px;text-align:center;">
# MAGIC     <div style="font-size:16pt;font-weight:700;color:#0b2026;margin-bottom:6px;">Catalogs</div>
# MAGIC     <div style="font-size:13pt;color:#0b2026;margin-bottom:14px;">Utilizing multiple catalogs, one for each environment.</div>
# MAGIC     <div style="background:#1B5162;color:#fff;border-radius:4px;padding:9px;text-align:center;font-weight:700;font-size:13pt;margin-bottom:8px;">DEV</div><div style="background:#1B5162;color:#fff;border-radius:4px;padding:9px;text-align:center;font-weight:700;font-size:13pt;margin-bottom:8px;">STAGE</div><div style="background:#1B5162;color:#fff;border-radius:4px;padding:9px;text-align:center;font-weight:700;font-size:13pt;margin-bottom:8px;">PROD</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - Isolating environments for the different stages of development is key to a CI/CD pipeline &mdash; code is developed and tested in development and staging before touching production.
# MAGIC - In Databricks, you can isolate environments using multiple workspaces (one per environment), multiple catalogs (one per environment), or both.
# MAGIC - The minimal setup is two environments (Development & Stage, and Production), but this varies with your organization's requirements.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B7. Setting Up Your Data for CI/CD
# MAGIC
# MAGIC Each environment uses data suited to its purpose &mdash; from small, anonymized sets in development to real, protected data in production.
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px;max-width:1120px;margin:0 auto;font-family:sans-serif;align-items:start;">
# MAGIC   <div style="background:#F9F7F4;border-radius:8px;box-shadow:0 2px 8px rgba(27,49,57,0.06);overflow:hidden;">
# MAGIC   <div style="background:#4299E0;color:#fff;font-size:15pt;font-weight:700;padding:12px 18px;">Dev Data</div>
# MAGIC   <ul style="margin:14px 18px 16px 18px;padding-left:18px;font-size:12.5pt;color:#0b2026;line-height:1.5;"><li>Often a small static subset of production data</li><li>Anonymized or synthetic datasets</li><li>Supports rapid development and testing</li><li>Ensures privacy and data integrity</li></ul>
# MAGIC </div>
# MAGIC   <div style="background:#F9F7F4;border-radius:8px;box-shadow:0 2px 8px rgba(27,49,57,0.06);overflow:hidden;">
# MAGIC   <div style="background:#F0A500;color:#0b2026;font-size:15pt;font-weight:700;padding:12px 18px;">Stage Data</div>
# MAGIC   <ul style="margin:14px 18px 16px 18px;padding-left:18px;font-size:12.5pt;color:#0b2026;line-height:1.5;"><li>Mirrors production structure &amp; volume, typically static</li><li>Anonymized or scrubbed of sensitive information</li><li>Ensures realistic testing and validation</li></ul>
# MAGIC </div>
# MAGIC   <div style="background:#F9F7F4;border-radius:8px;box-shadow:0 2px 8px rgba(27,49,57,0.06);overflow:hidden;">
# MAGIC   <div style="background:#98102A;color:#fff;font-size:15pt;font-weight:700;padding:12px 18px;">Prod Data</div>
# MAGIC   <ul style="margin:14px 18px 16px 18px;padding-left:18px;font-size:12.5pt;color:#0b2026;line-height:1.5;"><li>Live &amp; fully operational</li><li>Contains real user data, continuously updated</li><li>Requires high security, privacy &amp; compliance</li></ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - In development, data is often anonymized or synthetic, allowing rapid development and testing without compromising privacy or production data integrity.
# MAGIC - Staging data should closely mirror production in structure and volume, with anonymized or scrubbed sensitive information, to ensure realistic testing and validation.
# MAGIC - Production data is live, fully operational, and continuously updated with real user data, and must be handled with high security, privacy, and compliance standards.
# MAGIC - Each environment should have data suited to its purpose, balancing realism, security, and compliance; the implementation depends on your organization and data sensitivity.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B8. Deployment Tools in Databricks Overview
# MAGIC
# MAGIC Databricks offers three ways to automate deployment: the **REST API**, the **Databricks SDKs**, and the **Databricks CLI**. This course uses the CLI with Declarative Automation Bundles (DABs).
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px;max-width:1120px;margin:0 auto;font-family:sans-serif;align-items:start;">
# MAGIC   <div style="background:#fff;border:1px solid #DCE0E2;border-top:6px solid #618794;border-radius:8px;padding:16px 18px;box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="font-size:16pt;font-weight:700;color:#0b2026;text-align:center;margin-bottom:10px;">REST API</div>
# MAGIC   <ul style="margin:0;padding-left:18px;font-size:12.5pt;color:#0b2026;line-height:1.5;"><li>Provides direct access to Databricks functionality through HTTP requests</li><li>Requires <b>manual construction</b> of HTTP requests and handling responses</li></ul>
# MAGIC </div>
# MAGIC   <div style="background:#fff;border:1px solid #DCE0E2;border-top:6px solid #4299E0;border-radius:8px;padding:16px 18px;box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="font-size:16pt;font-weight:700;color:#0b2026;text-align:center;margin-bottom:10px;">Databricks SDK</div>
# MAGIC   <ul style="margin:0 0 8px 0;padding-left:18px;font-size:12.5pt;color:#0b2026;line-height:1.5;"><li>Accelerates development; covers all public REST API operations</li></ul><div style="font-size:12.5pt;color:#0b2026;margin-bottom:8px;">SDKs for:</div><div style="display:flex;flex-wrap:wrap;gap:6px;"><span style="background:rgba(66,153,224,0.10);border:1px solid #4299E0;border-radius:14px;padding:3px 12px;font-size:12.5pt;color:#0b2026;">Python</span><span style="background:rgba(66,153,224,0.10);border:1px solid #4299E0;border-radius:14px;padding:3px 12px;font-size:12.5pt;color:#0b2026;">Java</span><span style="background:rgba(66,153,224,0.10);border:1px solid #4299E0;border-radius:14px;padding:3px 12px;font-size:12.5pt;color:#0b2026;">Go</span><span style="background:rgba(66,153,224,0.10);border:1px solid #4299E0;border-radius:14px;padding:3px 12px;font-size:12.5pt;color:#0b2026;">R</span></div>
# MAGIC </div>
# MAGIC   <div style="background:#fff;border:2px solid #FF5F46;border-top:6px solid #FF5F46;border-radius:8px;padding:16px 18px;box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="font-size:16pt;font-weight:700;color:#0b2026;text-align:center;margin-bottom:10px;">Databricks CLI</div>
# MAGIC   <ul style="margin:0;padding-left:18px;font-size:12.5pt;color:#0b2026;line-height:1.5;"><li>Easy-to-use interface for automation from terminal, command prompt, or bash scripts</li><li>Use <b>Declarative Automation Bundles (DABs)</b> within the CLI to write infrastructure as code</li></ul><div style="background:rgba(255,95,70,0.15);border:1px solid #FF5F46;border-radius:6px;padding:4px 10px;font-size:11.5pt;font-weight:700;color:#98102A;text-align:center;margin-top:10px;">Method used in this course</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - The REST API provides direct access to Databricks functionality through HTTP requests, but requires manually constructing requests and handling responses.
# MAGIC - The Databricks SDK accelerates development, covers all public REST API operations, and supports Python, Java, Go, and R.
# MAGIC - The Databricks CLI offers an easy-to-use interface for automating tasks from the terminal, command prompt, or bash scripts, and lets you use Declarative Automation Bundles (DABs) to write infrastructure as code. This course focuses on the CLI with DABs.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B9. Using the Databricks CLI: Connection and Authentication
# MAGIC
# MAGIC There are three common ways to connect to and authenticate the Databricks CLI, depending on your needs. This course primarily uses the **notebook** approach.
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px;max-width:1120px;margin:0 auto;font-family:sans-serif;align-items:start;">
# MAGIC   <div style="background:#fff;border:1px solid #DCE0E2;border-top:6px solid #4299E0;border-radius:8px;padding:16px 18px;box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="font-size:16pt;font-weight:700;color:#0b2026;text-align:center;margin-bottom:10px;">Web Terminal</div>
# MAGIC   <ul style="margin:0;padding-left:18px;font-size:12.5pt;color:#0b2026;line-height:1.5;"><li>Run CLI commands from the Databricks web terminal</li><li>Enables shell commands in Databricks</li><li>Uses the latest version of the CLI by default</li><li>Authentication based on the current user</li><li>Must be enabled</li></ul>
# MAGIC </div>
# MAGIC   <div style="background:#fff;border:1px solid #DCE0E2;border-top:6px solid #F0A500;border-radius:8px;padding:16px 18px;box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="font-size:16pt;font-weight:700;color:#0b2026;text-align:center;margin-bottom:10px;">VS Code</div>
# MAGIC   <ul style="margin:0;padding-left:18px;font-size:12.5pt;color:#0b2026;line-height:1.5;"><li>Install and use the CLI locally</li><li>You must authenticate to Databricks</li><li>Use the Databricks VS Code Extension for additional features</li></ul>
# MAGIC </div>
# MAGIC   <div style="background:#fff;border:2px solid #FF5F46;border-top:6px solid #FF5F46;border-radius:8px;padding:16px 18px;box-shadow:0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="font-size:16pt;font-weight:700;color:#0b2026;text-align:center;margin-bottom:10px;">Databricks Notebook</div>
# MAGIC   <ul style="margin:0;padding-left:18px;font-size:12.5pt;color:#0b2026;line-height:1.5;"><li>Run shell commands with <code>%sh</code> in a notebook</li><li>Install and use the Databricks CLI</li><li>Authenticate with a token to use the CLI</li></ul><div style="background:rgba(255,95,70,0.15);border:1px solid #FF5F46;border-radius:6px;padding:4px 10px;font-size:11.5pt;font-weight:700;color:#98102A;text-align:center;margin-top:10px;">Main method used in this course</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - The Databricks web terminal runs CLI commands from the web interface, enables shell commands in Databricks, uses the latest CLI version by default, authenticates based on the current user, and must be enabled in your environment.
# MAGIC - You can install and run the CLI locally within VS Code &mdash; authenticate to your workspace, and use the Databricks VS Code Extension for additional features.
# MAGIC - In a Databricks notebook, you can run shell commands with `%sh`, install and use the CLI, and authenticate with a token. This course uses notebooks to run CLI commands for simplicity and ease of learning; a different approach may suit your organization in production.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Conclusion
# MAGIC
# MAGIC - DevOps continuously integrates, tests, and deploys code across the plan-code-build-test-release-deploy-operate-monitor lifecycle; applied to data engineering it becomes DataOps.
# MAGIC - CI merges and tests changes frequently; CD automates release, with delivery keeping a manual production gate and deployment automating it fully.
# MAGIC - Automated tests layer from fast unit tests up through integration to end-to-end system tests.
# MAGIC - Environments and data are isolated across DEV, STAGE, and PROD using separate workspaces and/or catalogs.
# MAGIC - Databricks provides the REST API, SDKs, and CLI to deploy projects; this course uses the CLI with DABs, run mainly from a notebook.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Before continuing, you must run the required setup notebook: `./02 - REQUIRED - Course Setup and Authentication`. It creates the isolated catalogs and configures Databricks CLI authentication used in every demo and lab. If you skip it, the rest of the class will not work correctly.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
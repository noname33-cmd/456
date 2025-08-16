# ... остальной код без изменений выше ...
# JBoss
JBOSS_CLI = os.environ.get("JBOSS_CLI", "/u01/jboss/bin/jboss-cli.sh")  # ← фикс по умолчанию
JBOSS_CONTROLLER = os.environ.get("JBOSS_CONTROLLER", "127.0.0.1:9990")
JBOSS_USER = os.environ.get("JBOSS_USER")
JBOSS_PASS = os.environ.get("JBOSS_PASS")
JBOSS_DEPLOYS = [x for x in (os.environ.get("JBOSS_DEPLOYS","").split(",")) if x.strip()]
# ... остальной код без изменений ниже ...

from churnexplainer import train
from churnexplainer.data import dataset, load_dataset
import cdsw
import os

os.environ.get('CHURN_MODEL_TYPE', 'linear') #| gb | nonlinear | voting"
#os.environ.get('CHURN_MODEL_TYPE', sys.argv[1])
os.environ.get('CHURN_DATASET', 'ibm') #| breastcancer | iris | telco
#os.environ.get('CHURN_DATASET', sys.argv[2])

train_score, test_score, model_path = train.experiment_and_save()

cdsw.track_metric("train_score",round(train_score,2))
cdsw.track_metric("test_score",round(test_score,2))
cdsw.track_metric("model_path",model_path)
cdsw.track_file(model_path)
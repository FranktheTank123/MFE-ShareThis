"""
Welcome to the GBT_library.

This library contains all generic functions
required for GBT model tuning.
"""

import numpy as np
import pandas as pd
import matplotlib.pylab as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter
# machine learning packages
from sklearn import cross_validation, metrics   # Additional scklearn functions
from sklearn.metrics import roc_curve, auc


# global paramter for plot
# sns.set_style("ticks")
sns.set(font_scale=1.5)
title_font = {'fontname': 'Arial', 'size': '20', 'color': 'black',
              'weight': 'bold', 'verticalalignment': 'bottom'}
axis_font = {'fontname': 'Arial', 'size': '14', 'weight': 'bold'}
legend_properties = {'size': '14', 'weight':'bold'}

def gbtDataCleasing(statement_vector_file, test_portion, predictors_bm, y,
                    drop_middle_thrid=False, feature_start=1, feature_end=300,
                    seed=1):
    """
    Data preprocessing.

    Parameters:
    statement_vector_file: trained statement vectors plus momentum factors
    test_portion: 0-1, in decimals
    drop_middle_thrid: whether the middle 1/3 should be dropped
    """
    # set seed
    np.random.seed(seed)

    # read the data
    vectors = pd.read_csv(statement_vector_file, index_col=0)

    # filter out the Nan's and take/not to take the middle ROEs
    if drop_middle_thrid:
        vectors_filtered = vectors[vectors['Label'] != 2].dropna()
    else:
        vectors_filtered = vectors.dropna()

    # add 2 more columns of the sign of the change
    vectors_filtered['ROE_excess_change_sign'] \
        = vectors_filtered['ROE_excess_change'] > 0
    vectors_filtered['ROE_change_sign'] = vectors_filtered['ROE_change'] > 0

    # check if there is any missing data
    assert vectors_filtered.isnull().sum().sum() == 0, \
        "there are some na's in df vectors_filtered"

    # how big is our super cleaned data
    print("Total DF shape: ", vectors_filtered.shape)
    m = len(vectors_filtered)
    colnames = vectors_filtered.columns.values

    # we augment the bm model by 300 features
    predictors = np.append(colnames[feature_start:feature_end+1], predictors_bm)

    # shuffle the data and take out 10% of data
    vectors_filtered_shuffled = vectors_filtered \
        .iloc[np.random.permutation(len(vectors_filtered))] \
        .reset_index(drop=True)

    test_data = vectors_filtered_shuffled.iloc[:int(m*test_portion)]
    train_valid_data = vectors_filtered_shuffled.iloc[int(m*test_portion):]

    # we want to make sure the % of +'s and -'s are close
    # otherwise we need to rebalance
    pos_ratio  \
        = (vectors_filtered_shuffled[y] > 0).sum() / \
        len(vectors_filtered_shuffled)

    print("The positive ratio is {:.4f}".format(pos_ratio))
    print("Data cleasing is comptled.")
    return train_valid_data, test_data, predictors


def modelfit(alg, dtrain, predictors, y, performCV=True,
             printFeatureImportance=True, cv_folds=5, num_features_show=30,
             model_name=''):
    """
    Main model fit function for GBT with cross validation.

    Parameters:

    alg: a model, such as GradientBoostingClassifier
    dtrain: trainning data set, including both x's and y
    predictors: vectors of columns indeices of the x's
    y: column index of y
    performCV: wheather you want to perform CV
    """
    # Fit the algorithm on the data
    alg.fit(dtrain[predictors], dtrain[y])

    # Predict training set:
    dtrain_predictions = alg.predict(dtrain[predictors])
    dtrain_predprob = alg.predict_proba(dtrain[predictors])[:, 1]

    # Perform cross-validation:
    if performCV:
        cv_score = cross_validation.cross_val_score(alg, dtrain[predictors],
                                                    dtrain[y], cv=cv_folds,
                                                    scoring='roc_auc')
    # Print model report:
    print ("\nModel Report")
    print ("Accuracy : %.4g" % metrics.accuracy_score(dtrain[y].values,
           dtrain_predictions))
    print ("AUC Score (Train): %f"
           % metrics.roc_auc_score(dtrain[y], dtrain_predprob))

    if performCV:
        print ("CV Score : Mean - %.7g | Std - %.7g | Min - %.7g | Max - %.7g"
               % (np.mean(cv_score), np.std(cv_score), np.min(cv_score),
                  np.max(cv_score)))

    # Print Feature Importance:
    if printFeatureImportance:
        feat_imp = pd.Series(alg.feature_importances_, predictors) \
            .sort_values(ascending=False)[:num_features_show]
        feat_imp.plot(kind='bar')
        plt.ylabel('Feature Importance Score', **axis_font)
        plt.title('Feature Importances\n{}'.format(model_name), **title_font)


def testThresholdVersusAccuracy(alg, dtest, predictors, y):
    '''
    THIS FUNCTION IS DEPRECATED.

    this function returns 6 plots to describe the testing dataset

    -- the first 2 plot shows the distribution of confidence of
        the predictions: pos and neg separately
    -- the 3rd plot shows the effective number of data included
        (in percentage) when we increase the threshold, in other words,
        we only bet when the confidence is above a certain threshold
    -- the 4-6th plot shows the (total, positive, negative) accuracies
        vs different threshold
    '''
    # get the test prediction and probability
    test_pred_prob = alg.predict_proba(dtest[predictors])
    test_pred = alg.predict(dtest[predictors])
    total_counts = len(test_pred)
    total_counts_pos = (test_pred == 1).sum()
    total_counts_neg = (test_pred == 0).sum()

    thresholds = np.linspace(0.5, 0.95, 46)
    accuracies = np.empty(46)
    accuracies_pos = np.empty(46)
    accuracies_neg = np.empty(46)
    counts = np.empty(46)
    counts_pos = np.empty(46)
    counts_neg = np.empty(46)

    for i, threshold in enumerate(thresholds):
        filter_pos_ = np.logical_and(test_pred == 1,
                                     test_pred_prob[:, 1] >= threshold)
        filter_neg_ = np.logical_and(test_pred == 0,
                                     test_pred_prob[:, 0] >= threshold)
        filter_joint_ = np.logical_or(filter_pos_, filter_neg_)
        accuracies_pos[i] = \
            metrics.accuracy_score(dtest[y].values[filter_pos_],
                                   test_pred[filter_pos_])
        accuracies_neg[i] = \
            metrics.accuracy_score(dtest[y].values[filter_neg_],
                                   test_pred[filter_neg_])
        accuracies[i] = \
            metrics.accuracy_score(dtest[y].values[filter_joint_],
                                   test_pred[filter_joint_])
        counts[i] = filter_joint_.sum()/total_counts
        counts_pos[i] = filter_pos_.sum()/total_counts_pos
        counts_neg[i] = filter_neg_.sum()/total_counts_neg

    # plots
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(221)
    ax.hist(test_pred_prob[test_pred == 0][:, 0])
    plt.title('Negative prediction confidence')
    plt.xlabel('probability of confidence')
    plt.ylabel('counts')
    plt.grid(True)

    ax = fig.add_subplot(222)
    ax.hist(test_pred_prob[test_pred == 1][:, 1])
    plt.title('Positive prediction confidence')
    plt.xlabel('probability of confidence')
    plt.ylabel('counts')
    plt.grid(True)

    ax = fig.add_subplot(223)
    ax.plot(thresholds, counts, label='Total Counts')
    ax.plot(thresholds, counts_pos, label='Pos Counts')
    ax.plot(thresholds, counts_neg, label='Neg Counts')
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'
                                               .format(y)))
    plt.ylim(0, 1)
    plt.title('Thresholds vs Effective counts')
    plt.xlabel('Thresholds')
    plt.ylabel('Effective Counts in percentage')
    plt.legend(loc='upper right')
    plt.grid(True)

    ax = fig.add_subplot(224)
    ax.plot(thresholds, accuracies, label='Accuracy')
    ax.plot(thresholds, accuracies_pos, label='Positive Accuracy')
    ax.plot(thresholds, accuracies_neg, label='Negative Accuracy')
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'
                                               .format(y)))
    plt.ylim(0.5, 1.2)
    plt.title('Thresholds vs Accuracies')
    plt.xlabel('Thresholds')
    plt.ylabel('Accuracy')
    plt.legend(loc='upper left')
    plt.grid(True)

    plt.tight_layout()


def getAUC(alg, dtest, predictors, y, threshold=0.5,
           show_plot=True, title_second_line=""):
    """Get/plot AUC for a given threshold."""
    # get the test prediction and probability
    test_pred_prob = alg.predict_proba(dtest[predictors])
    test_pred = alg.predict(dtest[predictors])

    filter_pos_ = np.logical_and(test_pred == 1,
                                 test_pred_prob[:, 1] >= threshold)
    filter_neg_ = np.logical_and(test_pred == 0,
                                 test_pred_prob[:, 0] >= threshold)
    filter_joint_ = np.logical_or(filter_pos_, filter_neg_)
    try:
        false_positive_rate, true_positive_rate, thresholds = \
            roc_curve(dtest[y].values[filter_joint_], test_pred[filter_joint_])
        roc_auc = auc(false_positive_rate, true_positive_rate)
    except:
        false_positive_rate, true_positive_rate = 0, 1
        roc_auc = 1

    total_counts = len(test_pred)
    data_included = filter_joint_.sum()/1./total_counts

    if show_plot:
        plt.title("Receiver Operating Characteristic\n Threshold={} \n{}"
                  .format(threshold, title_second_line), **title_font)
        plt.plot(false_positive_rate, true_positive_rate, 'b',
                 label='AUC = %0.2f' % roc_auc)
        plt.legend(loc='lower right')
        plt.plot([0, 1], [0, 1], 'r--')
        plt.xlim([-0.1, 1.2])
        plt.ylim([-0.1, 1.2])
        plt.ylabel('True Positive Rate', **axis_font)
        plt.xlabel('False Positive Rate', **axis_font)
        plt.show()
    return roc_auc, data_included


def AUCvsThresholdPlot(alg, dtest, predictors, y, model_name="",
                       alg2=None, dtest2=None, predictors2=None, y2=None,
                       model_name2="", alg3=None, dtest3=None, predictors3=None, 
                       y3=None, model_name3="",
                       title_second_line=""):
    """
    Plot AUC, % of counts vs Threshold.

    2nd model supported.
    """
    sns.set_style("ticks")

    # plot the auc, % of count vs threshold
    threshold = np.linspace(0.5, 0.95, 46)
    aucs = np.empty(46)
    fraction = np.empty(46)

    for (i, t_) in enumerate(threshold):
        aucs[i], fraction[i] = getAUC(alg, dtest, predictors, y, t_, False)

    fig, ax = plt.subplots(figsize=(14, 8))
    ax.plot(threshold, aucs, "blue", linewidth=2,
            label="Area Under Curve - {}".format(model_name))
    ax.set_xlabel('Threshold', **axis_font)
    ax.set_title("Area Under Curve and Effective Count vs Threshold\n{}"
                 .format(title_second_line), **title_font)

    ax.set_ylabel('Area Under Curve', **axis_font)
    ax.set_ylim(0.5, 1)
    ax.set_xlim(threshold[0], threshold[-1])

    if(alg2 is not None):
        aucs_2 = np.empty(46)
        fraction_2 = np.empty(46)
        for (i, t_) in enumerate(threshold):
            aucs_2[i], fraction_2[i] = getAUC(alg2, dtest2,
                                              predictors2, y2, t_, False)
        ax.plot(threshold, aucs_2, "red", linewidth=2,
                label="Area Under Curve - {}".format(model_name2))

    if(alg3 is not None):
        aucs_3 = np.empty(46)
        fraction_3 = np.empty(46)
        for (i, t_) in enumerate(threshold):
            aucs_3[i], fraction_3[i] = getAUC(alg3, dtest3,
                                              predictors3, y3, t_, False)
        ax.plot(threshold, aucs_3, "green", linewidth=2,
                label="Area Under Curve - {}".format(model_name3))

    ax.legend(loc="upper right", prop=legend_properties)

    ax2 = ax.twinx()
    ax2.plot(threshold, fraction, "blue", linestyle='--', linewidth=2,
             label="% Data included - {}".format(model_name))
    ax2.set_ylabel('Data included in percentage', **axis_font)
    ax2.set_xlim(threshold[0], threshold[-1])
    ax2.set_ylim(0, 1)
    if(alg2 is not None):
        ax2.plot(threshold, fraction_2, "red", linestyle='--', linewidth=2,
                 label="% Data included - {}".format(model_name2))
    if(alg3 is not None):
        ax2.plot(threshold, fraction_3, "green", linestyle='--', linewidth=2,
                 label="% Data included - {}".format(model_name3))
    ax2.legend(loc="upper left", prop=legend_properties)


def AUCvsCountsPlot(alg, dtest, predictors, y, model_name="",
                       alg2=None, dtest2=None, predictors2=None, y2=None,
                       alg3=None, dtest3=None, predictors3=None, 
                       y3=None, model_name3="",
                       model_name2="", title_second_line=""):
    """
    Plot AUC vs % of counts

    2nd model supported.
    """
    sns.set_style("ticks")

    # plot the auc, % of count vs threshold
    threshold = np.linspace(0.5, 0.95, 46)
    aucs = np.empty(46)
    fraction = np.empty(46)

    for (i, t_) in enumerate(threshold):
        aucs[i], fraction[i] = getAUC(alg, dtest, predictors, y, t_, False)

    fig, ax = plt.subplots(figsize=(14, 8))
    ax.invert_xaxis() # inverse x-axis
    ax.plot(fraction, aucs, "blue", linewidth=2,
            label="Area Under Curve - {}".format(model_name))
    ax.set_xlabel('% Data included', **axis_font)
    ax.set_title("Area Under Curve vs Effective Count\n{}"
                 .format(title_second_line), **title_font)

    ax.set_ylabel('Area Under Curve', **axis_font)
    print(aucs)
    ax.set_ylim(0.5, 1)
    #ax.set_xlim(threshold[0], threshold[-1])

    if(alg2 is not None):
        aucs_2 = np.empty(46)
        fraction_2 = np.empty(46)
        for (i, t_) in enumerate(threshold):
            aucs_2[i], fraction_2[i] = getAUC(alg2, dtest2,
                                              predictors2, y2, t_, False)
        ax.plot(fraction, aucs_2, "red", linewidth=2,
                label="Area Under Curve - {}".format(model_name2))
    if(alg3 is not None):
        aucs_3 = np.empty(46)
        fraction_3 = np.empty(46)
        for (i, t_) in enumerate(threshold):
            aucs_3[i], fraction_3[i] = getAUC(alg3, dtest3,
                                              predictors3, y3, t_, False)
        ax.plot(fraction, aucs_3, "green", linewidth=2,
                label="Area Under Curve - {}".format(model_name3))

    ax.legend(loc="upper left", prop=legend_properties)

    



def accuracyGivenThreshold(alg, dtest, predictors, y,
                           pos_threshold, neg_threshold=-1):
    '''
    Returns specific accuracies given specific thresholds.
    '''
    test_pred_prob = alg.predict_proba(dtest[predictors])
    test_pred = alg.predict(dtest[predictors])

    # check if neg)threshold is specified, otherwise, set to pos_threshold
    if neg_threshold == -1:
        neg_threshold = pos_threshold

    filter_pos_ = np.logical_and(test_pred == 1,
                                 test_pred_prob[:, 1] >= pos_threshold)
    filter_neg_ = np.logical_and(test_pred == 0,
                                 test_pred_prob[:, 0] >= neg_threshold)
    filter_joint_ = np.logical_or(filter_pos_, filter_neg_)
    accuracy_pos = metrics.accuracy_score(dtest[y].values[filter_pos_],
                                          test_pred[filter_pos_])
    accuracy_neg = metrics.accuracy_score(dtest[y].values[filter_neg_],
                                          test_pred[filter_neg_])
    accuracy = metrics.accuracy_score(dtest[y].values[filter_joint_],
                                      test_pred[filter_joint_])
    counts = filter_joint_.sum()

    return (accuracy, counts, accuracy_pos, accuracy_neg)

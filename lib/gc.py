"""
This library consolidates gnucash specific functions.
"""
import datetime


def add_result_to_xl(xl_arr, res):
    """
    This function will get the result dictionary as prepared by handle_account and convert this to an array of
    dictionaries where each dictionary is a line that needs to be written to excel output.

    A summary line will be added to each subcategory and category.

    :param xl_arr: Output array with a dictionary line per output line.
    :param res:
    :return:
    """
    prev_cat = ""
    prev_sub = ""
    for k in sorted(res, key=lambda v: v.lower()):
        xl_dic = {}
        acclvls = k.split(":")
        cat = acclvls[0]
        sub = acclvls[1]

        if prev_cat:
            if cat != prev_cat:
                if prev_sub:
                    summary(xl_arr, "{cat}:{sub}".format(cat=prev_cat, sub=prev_sub), res)
                    prev_sub = ""
                summary(xl_arr, prev_cat, res)
                xl_dic["Categorie"] = cat.upper()
            else:
                xl_dic["Categorie"] = ""
        else:
            xl_dic["Categorie"] = cat.upper()
        prev_cat = cat

        if prev_sub:
            if sub != prev_sub:
                lbl = "{cat}:{sub}".format(cat=prev_cat, sub=prev_sub)
                summary(xl_arr, lbl, res)
        prev_sub = sub
        xl_dic["Subcategorie"] = sub
        if len(acclvls) > 2:
            xl_dic["Detail"] = k[len(acclvls[0]) + len(acclvls[1]) + 2:]
        else:
            xl_dic["Detail"] = ""
        xl_dic["Bedrag"] = res[k]
        xl_dic["Totaal"] = ""

        xl_arr.append(xl_dic)
    summary(xl_arr, "{cat}:{sub}".format(cat=prev_cat, sub=prev_sub), res)
    summary(xl_arr, prev_cat, res)
    return


def handle_account(start, end, results, acc, lbl=None):
    """
    This function will calculate the sum of transactions in an account and all sub-accounts during a period of time.

    :param start: First day YYYY-MM-DD for accounting
    :param end: Last day YYYY-MM-DD for accounting
    :param results: Dictionary with key full account name and value total amount for this period.
    :param acc: Account name
    :param lbl: Account name prefix. Full account name is lbl:acc
    :return:
    """
    splits = [split.value for split in acc.splits
              if (split.transaction.post_date >= datetime.datetime.strptime(start, "%Y-%m-%d").date()) and
              (split.transaction.post_date < datetime.datetime.strptime(end, "%Y-%m-%d").date())]
    tot = sum(splits)
    if lbl:
        lbl = "{lbl}:{a}".format(lbl=lbl, a=acc.name)
    else:
        lbl = acc.name
    if tot != 0:
        results[lbl] = tot * (-1)
    for child in acc.children:
        handle_account(start, end, results, child, lbl)
    return


def summary(out_arr, lbl, res_arr=None, total=None):
    """
    This function will add a summary line to the result excel. A summary line can be for subcategory, category, all
    categories and document

    :param out_arr:
    :param lbl: If label has ':' (category separator), then output is for subcategory. If label is 'Report', then
    output is for Overall Report. If label is 'Categories' then output is for category group in the report. Otherwise
    output is for the Category. For Report the total sum needs to be provided, otherwise it will be calculated.
    :param res_arr: Result of the  handle_account function. Required for Category and Subcategory
    :param total: Sum of the accounts for this category. This needs to be provided for Report and for Categories.
    :return:
    """
    out_dic = dict(Categorie="", Subcategorie="", Detail="", Bedrag="", Totaal="")
    if lbl == "Report":
        out_dic["Categorie"] = "Totaal Rapport"
        out_dic["Totaal"] = total
    elif lbl == "Categories":
        out_dic["Categorie"] = "Totaal Groep"
        out_dic["Totaal"] = sum([res_arr[k] for k in res_arr])
    elif ":" in lbl:
        out_dic["Detail"] = "Totaal"
        out_dic["Totaal"] = sum([res_arr[k] for k in res_arr if lbl == k[:len(lbl)]])
    else:
        out_dic["Categorie"] = "Totaal {cat}".format(cat=lbl)
        out_dic["Totaal"] = sum([res_arr[k] for k in res_arr if lbl == k[:len(lbl)]])
    out_arr.append(out_dic)
    return out_dic["Totaal"]

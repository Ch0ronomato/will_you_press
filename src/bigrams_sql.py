class Bigram:
    def __init__(self, w1, w2):
        print("Constructing bigram {0} {1}".format(w1, w2))
        self.w1 = w1
        self.w2 = w2

    def __str__(self):
        return "(w1: {0}, w2: {1})".format(self.w1, self.w2)


class Bigrams:
    def __init__(self, bigrams_raw):
        if 'result' not in bigrams_raw or \
           'dilemma' not in bigrams_raw or \
           'total' not in bigrams_raw:
                raise ValueError("Keys not in argument")
        self.result = [Bigram(x[0], x[1]) if len(x) else Bigram("", "") for x in bigrams_raw['result']]
        self.dilemma = [Bigram(x[0], x[1]) if len(x) else Bigram("", "") for x in bigrams_raw['dilemma']]
        self.total = [Bigram(x[0], x[1]) if len(x) else Bigram("", "") for x in bigrams_raw['total']]

    def __str__(self):
        return "result: " + str([str(x) for x in self.result])\
               + " dilemma: " + str([str(x) for x in self.dilemma])\
               + " total: " + str([str(x) for x in self.total])


class Question:
    def __init__(self, qid, question_text, answers, yeses, bigrams):
        self.qid = qid
        self.question_text = question_text
        self.answers = answers
        self.yeses = yeses
        self.question_bigrams = Bigrams(bigrams)

    def __getitem__(self, item):
        if item == 'qid':
            return self.qid
        if item == 'question_text':
            return self.question_text
        if item == 'answers':
            return self.answers
        if item == 'yeses':
            return self.yeses
        if item == 'question_bigrams':
            return self.question_bigrams

    def __str__(self):
        return "qid: {0} text: {1} answers: {2} yeses: {3} bigrams: ".format(
            self.qid,
            self.question_text,
            self.answers,
            self.yeses,
        ) + str(self.question_bigrams)

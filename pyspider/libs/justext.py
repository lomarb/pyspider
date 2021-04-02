# Copyright (c) 2011 Jan Pomikalek
# All rights reserved.
#
# This software is licensed as described here: http://corpus.tools/wiki/Justext

"""
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
        SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import codecs
import os
import re
import sys

from xml.sax.handler import ContentHandler

import lxml.etree  # type: ignore
import lxml.html  # type: ignore
import lxml.sax  # type: ignore

MAX_LINK_DENSITY_DEFAULT = 0.2
LENGTH_LOW_DEFAULT = 70
LENGTH_HIGH_DEFAULT = 200
STOPWORDS_LOW_DEFAULT = 0.30
STOPWORDS_HIGH_DEFAULT = 0.32
NO_HEADINGS_DEFAULT = False
# Short and near-good headings within MAX_HEADING_DISTANCE characters before
# a good paragraph are classified as good unless --no-headings is specified.
MAX_HEADING_DISTANCE_DEFAULT = 200
#Maximum distance (in paragraphs) of a short paragraph from a good paragraph
#to re-classify the short paragraph as good.
MAX_GOOD_DISTANCE_DEFAULT = 5
PARAGRAPH_TAGS = ['blockquote', 'caption', 'center', 'col', 'colgroup', 'dd',
        'div', 'dl', 'dt', 'fieldset', 'form', 'legend', 'optgroup', 'option',
        'p', 'pre', 'table', 'td', 'textarea', 'tfoot', 'th', 'thead', 'tr',
        'ul', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']
DEFAULT_ENCODING = 'utf-8'
DEFAULT_ENC_ERRORS = 'replace'
WHITE_SPACE_RE = re.compile('(?:\s|\xc2\xa0)+', re.UNICODE)
#Python2 RE do not match no-break space using \s

class JustextError(Exception):
    "Base class for jusText exceptions."


TAMERLON_SUPPORTED_LANGUAGES = {
    "en": "English",
    "en-GB": "English",
    "en-US": "English",
    "cs": "Czech",
    "de": "German",
    "ar": "Arabic",
    "nl": "Dutch",
    "fr": "French",
    "ru": "Russian",
    "es": "Spanish",
    "pt": "Portuguese",
    "pl": "Polish",
    "it": "Italian",
    "zh-CN": "Chinese_Simplified",
    "zh-TW": "Chinese_Traditional",
}


def get_stoplist(language):
    "Returns an inbuilt stoplist for the language as a set of words."
    stoplist_contents = open(os.path.join(os.path.dirname(__file__), 'stoplists', language + ".txt")).read()
    return set(l.strip().lower() for l in stoplist_contents.split(u'\n'))

def decode_html(html_string, encoding=None, default_encoding=DEFAULT_ENCODING,
        errors=DEFAULT_ENC_ERRORS):
    """
    Converts a string containing an HTML page (html_string) into unicode.
    Tries to guess character encoding from meta tags.
    """
    if encoding:
        return html_string.decode(encoding, errors=errors)
    re_meta1 = re.compile('''<meta\s+http-equiv=['"]?content-type['"]?\s+content=['"]?[^'"]*charset=([^'"]+)''', re.I)
    re_meta2 = re.compile('''<meta\s+content=['"]?[^'"]*charset=([^'"]+)['"]?\s+http-equiv=['"]?content-type['"]?''', re.I)
    re_meta3 = re.compile('''<meta\s+http-equiv=['"]?charset['"]?\s+content=['"]?([^'"]+)''', re.I)
    re_meta4 = re.compile('''<meta\s+content=['"]?([^'"]+)['"]?\s+http-equiv=['"]?charset['"]?''', re.I)
    re_meta5 = re.compile('''<meta\s+charset=['"]?([^'"]+)''', re.I)
    for re_meta in (re_meta1, re_meta2, re_meta3, re_meta4, re_meta5):
        m = re_meta.search(html_string)
        if m:
            meta_encoding = m.group(1)
            try:
                return html_string.decode(meta_encoding, errors=errors)
            except LookupError:
                # if the encoding specified in <meta> is unknown
                # proceed as if it wasn't found at all
                pass
    try:
        # if unknown encoding, try utf-8 first
        return html_string.decode('utf-8', errors='strict')
    except UnicodeDecodeError:
        # use default encoding if utf-8 failed
        try:
            return html_string.decode(default_encoding, errors=errors)
        except UnicodeDecodeError as e:
            raise JustextError('Unable to convert the HTML to unicode from %s: %s' % (
                default_encoding, e))

decode_entities_pp_trans = {
    ord(u'\x83'): u'\u0192',
    ord(u'\x84'): u'\u201e',
    ord(u'\x85'): u'\u2026',
    ord(u'\x86'): u'\u2020',
    ord(u'\x87'): u'\u2021',
    ord(u'\x88'): u'\u02c6',
    ord(u'\x89'): u'\u2030',
    ord(u'\x8a'): u'\u0160',
    ord(u'\x8b'): u'\u2039',
    ord(u'\x8c'): u'\u0152',
    ord(u'\x91'): u'\u2018',
    ord(u'\x92'): u'\u2019',
    ord(u'\x93'): u'\u201c',
    ord(u'\x94'): u'\u201d',
    ord(u'\x95'): u'\u2022',
    ord(u'\x96'): u'\u2013',
    ord(u'\x97'): u'\u2014',
    ord(u'\x98'): u'\u02dc',
    ord(u'\x99'): u'\u2122',
    ord(u'\x9a'): u'\u0161',
    ord(u'\x9b'): u'\u203a',
    ord(u'\x9c'): u'\u0153',
    ord(u'\x9f'): u'\u0178',
}
def decode_entities_pp(unicode_string):
    """
    Post-processing of HTML entity decoding. The entities &#128; to &#159;
    (&#x80; to &#x9f;) are not defined in HTML 4, but they are still used on
    the web and recognised by web browsers. This method converts some of the
    u'\x80' to u'\x9f' characters (which are likely to be incorrectly decoded
    entities; mostly control characters) to the characters which the entities
    are normally decoded to.
    """
    return unicode_string.translate(decode_entities_pp_trans)

def add_kw_tags(root):
    """
    Surrounds text nodes with <kw></kw> tags. To protect text nodes from
    being removed with nearby tags.
    """
    blank_text = re.compile(u'^\s*$', re.U)
    nodes_with_text = []
    nodes_with_tail = []
    for node in root.iter():
        # temporary workaround for issue #2 caused by a bug #690110 in lxml
        try:
            node.text
        except UnicodeDecodeError:
            # remove any text that can't be decoded
            node.text = ''

        if node.text and node.tag not in (lxml.etree.Comment, lxml.etree.ProcessingInstruction):
            nodes_with_text.append(node)
        if node.tail:
            nodes_with_tail.append(node)
    for node in nodes_with_text:
        kw = lxml.etree.Element('kw')
        if blank_text.match(node.text):
            kw.text = ' '
        else:
            kw.text = node.text
        node.text = None
        node.insert(0, kw)
    for node in nodes_with_tail:
        kw = lxml.etree.Element('kw')
        if blank_text.match(node.tail):
            kw.text = ' '
        else:
            kw.text = node.tail
        node.tail = None
        parent = node.getparent()
        parent.insert(parent.index(node) + 1, kw)
    return root

def remove_comments(root):
    "Removes comment nodes."
    to_be_removed = []
    for node in root.iter():
        if node.tag == lxml.etree.Comment:
            to_be_removed.append(node)
    for node in to_be_removed:
        parent = node.getparent()
        del parent[parent.index(node)]

def get_html_root(html_text, encoding=None, default_encoding=DEFAULT_ENCODING,
        enc_errors=DEFAULT_ENC_ERRORS):
    "Converts HTML text to HTML DOM root and returns the root."
    uhtml_text = html_text # decode_html(html_text, encoding, default_encoding, enc_errors)
    try:
        html_root = lxml.html.fromstring(uhtml_text)
    except ValueError: # Unicode strings with encoding declaration are not supported.
        # for XHTML files with encoding declaration, use the declared encoding
        html_root = lxml.html.fromstring(html_text)
    return html_root

def preprocess_html_root(html_root):
    "Removes unwanted parts from HTML DOM root and returns the cleaned root."
    # add <kw> tags, protect text nodes
    add_kw_tags(html_root)
    # remove comments
    remove_comments(html_root)
    # remove head, script and style
    to_be_removed = []
    for node in html_root.iter():
        if node.tag in ['head', 'script', 'noscript', 'style', 'figure', 'dialog',
                'footer', 'nav', 'canvas', 'svg', 'audio', 'embed', 'aside',
                'code', 'data', 'menu', 'object', 'picture', 'pre']:
            to_be_removed.append(node)
    for node in to_be_removed:
        parent = node.getparent()
        del parent[parent.index(node)]
    return html_root

def preprocess(html_text, encoding=None, default_encoding=DEFAULT_ENCODING,
        enc_errors=DEFAULT_ENC_ERRORS):
    "Converts HTML text to HTML DOM root, removes unwanted parts and returns the cleaned root."
    html_root = get_html_root(html_text, encoding, default_encoding, enc_errors)
    return preprocess_html_root(html_root)

class SaxParagraphMaker(ContentHandler):
    """
    A class for converting a HTML page represented as a DOM object into a list
    of paragraphs.
    """

    def __init__(self):
        self.dom = []
        self.paragraphs = []
        self.paragraph = {}
        self.link = False
        self.br = False
        self._start_new_pragraph()

    def _start_new_pragraph(self):
        if self.paragraph and self.paragraph['text_nodes'] != []:
            self.paragraph['text'] = WHITE_SPACE_RE.sub(' ', (
                ''.join(self.paragraph['text_nodes']))).strip()
            self.paragraphs.append(self.paragraph)
        self.paragraph = {
            'dom_path': '.'.join(self.dom),
            'text_nodes': [],
            'word_count': 0,
            'linked_char_count': 0,
            'tag_count': 0,
        }

    def startElementNS(self, name, qname, attrs):
        dummy_uri, name = name
        self.dom.append(name)
        if name == 'br':
            self.paragraph['text_nodes'].append(' ')
        if name in PARAGRAPH_TAGS or (name == 'br' and self.br):
            if name == 'br':
                # the <br><br> is a paragraph separator and should
                # not be included in the number of tags within the
                # paragraph
                self.paragraph['tag_count'] -= 1
            self._start_new_pragraph()
        else:
            if name == 'br':
                self.br = True
            else:
                self.br = False
            if name == 'a':
                self.link = True
            self.paragraph['tag_count'] += 1

    def endElementNS(self, name, qname):
        dummy_uri, name = name
        self.dom.pop()
        if name in PARAGRAPH_TAGS:
            self._start_new_pragraph()
        if name == 'a':
            self.link = False

    def endDocument(self):
        self._start_new_pragraph()

    def characters(self, content):
        text = WHITE_SPACE_RE.sub(' ', content)
        self.paragraph['text_nodes'].append(text)
        words = text.strip().split()
        self.paragraph['word_count'] += len(words)
        if self.link:
            self.paragraph['linked_char_count'] += len(text)
        self.br = False

def make_paragraphs(root):
    "Converts DOM into paragraphs."
    handler = SaxParagraphMaker()
    lxml.sax.saxify(root, handler)
    return [p for p in handler.paragraphs if p['text']]

def classify_paragraphs(paragraphs, stoplist, length_low=LENGTH_LOW_DEFAULT,
        length_high=LENGTH_HIGH_DEFAULT, stopwords_low=STOPWORDS_LOW_DEFAULT,
        stopwords_high=STOPWORDS_HIGH_DEFAULT, max_link_density=MAX_LINK_DENSITY_DEFAULT,
        no_headings=NO_HEADINGS_DEFAULT):
    "Context-free pragraph classification."
    for paragraph in paragraphs:
        length = len(paragraph['text'])
        stopword_count = 0
        for word in paragraph['text'].lower().split():
            if word in stoplist:
                stopword_count += 1
        word_count = paragraph['word_count']
        if word_count == 0:
            stopword_density = 0
            link_density = 0
        else:
            stopword_density = 1.0 * stopword_count / word_count
            link_density = float(paragraph['linked_char_count']) / length
        paragraph['stopword_count'] = stopword_count
        paragraph['stopword_density'] = stopword_density
        paragraph['link_density'] = link_density

        paragraph['heading'] = bool(not no_headings and re.search('(^h\d|\.h\d)', paragraph['dom_path']))
        if link_density > max_link_density:
            paragraph['cfclass'] = 'bad'
        elif (u'\xa9' in paragraph['text']) or ('&copy' in paragraph['text']):
            paragraph['cfclass'] = 'bad'
        elif re.search('(^select|\.select)', paragraph['dom_path']):
            paragraph['cfclass'] = 'bad'
        else:
            if length < length_low:
                if paragraph['linked_char_count'] > 0:
                    paragraph['cfclass'] = 'bad'
                else:
                    paragraph['cfclass'] = 'short'
            else:
                if stopword_density >= stopwords_high:
                    if length > length_high:
                        paragraph['cfclass'] = 'good'
                    else:
                        paragraph['cfclass'] = 'neargood'
                elif stopword_density >= stopwords_low:
                    paragraph['cfclass'] = 'neargood'
                else:
                    paragraph['cfclass'] = 'bad'

def _get_neighbour(i, paragraphs, ignore_neargood, inc, boundary):
    while i + inc != boundary:
        i += inc
        c = paragraphs[i]['class']
        if c in ['good', 'bad']:
            return c
        if c == 'neargood' and not ignore_neargood:
            return c
    return 'bad'

def get_prev_neighbour(i, paragraphs, max_good_distance, ignore_neargood):
    """
    Return the class of the paragraph at the top end of the short/neargood
    paragraphs block, up to max_good_distance paragraphs far. If ignore_neargood
    is True, than only 'bad' or 'good' can be returned, otherwise 'neargood'
    can be returned, too.
    """
    block_boundary = max(i - max_good_distance, -1)
    return _get_neighbour(i, paragraphs, ignore_neargood, -1, block_boundary)

def get_next_neighbour(i, paragraphs, max_good_distance, ignore_neargood):
    """
    Return the class of the paragraph at the bottom end of the short/neargood
    paragraphs block, up to max_good_distance paragraphs far. If ignore_neargood
    is True, than only 'bad' or 'good' can be returned, otherwise 'neargood'
    can be returned, too.
    """
    block_boundary = min(i + max_good_distance, len(paragraphs))
    return _get_neighbour(i, paragraphs, ignore_neargood, 1, block_boundary)

def revise_paragraph_classification(paragraphs, max_good_distance=MAX_GOOD_DISTANCE_DEFAULT,
        max_heading_distance=MAX_HEADING_DISTANCE_DEFAULT):
    """
    Context-sensitive paragraph classification. Assumes that classify_pragraphs
    has already been called.
    """
    # copy classes
    for paragraph in paragraphs:
        paragraph['class'] = paragraph['cfclass']

    # good headings
    for i, paragraph in enumerate(paragraphs):
        if not (paragraph['heading'] and paragraph['class'] == 'short'):
            continue
        j = i + 1
        distance = 0
        while j < len(paragraphs) and distance <= max_heading_distance:
            if paragraphs[j]['class'] == 'good':
                paragraph['class'] = 'neargood'
                break
            distance += len(paragraphs[j]['text'])
            j += 1

    # classify short
    new_classes = {}
    for i, paragraph in enumerate(paragraphs):
        if paragraph['class'] != 'short':
            continue
        prev_neighbour = get_prev_neighbour(i, paragraphs, max_good_distance, ignore_neargood=True)
        next_neighbour = get_next_neighbour(i, paragraphs, max_good_distance, ignore_neargood=True)
        neighbours = set((prev_neighbour, next_neighbour))
        if neighbours == set(['good']):
            new_classes[i] = 'good'
        elif neighbours == set(['bad']):
            new_classes[i] = 'bad'
        # it must be set(['good', 'bad'])
        elif (prev_neighbour == 'bad' and get_prev_neighbour(i, paragraphs, max_good_distance,
                ignore_neargood=False) == 'neargood') or \
                (next_neighbour == 'bad' and get_next_neighbour(i, paragraphs, max_good_distance,
                ignore_neargood=False) == 'neargood'):
            new_classes[i] = 'good'
        else:
            new_classes[i] = 'bad'

    for i, c in new_classes.items():
        paragraphs[i]['class'] = c

    # revise neargood
    for i, paragraph in enumerate(paragraphs):
        if paragraph['class'] != 'neargood':
            continue
        prev_neighbour = get_prev_neighbour(i, paragraphs, max_good_distance, ignore_neargood=True)
        next_neighbour = get_next_neighbour(i, paragraphs, max_good_distance, ignore_neargood=True)
        if (prev_neighbour, next_neighbour) == ('bad', 'bad'):
            paragraph['class'] = 'bad'
        else:
            paragraph['class'] = 'good'

    # more good headings
    for i, paragraph in enumerate(paragraphs):
        if not (paragraph['heading'] and paragraph['class'] == 'bad' and paragraph['cfclass'] != 'bad'):
            continue
        j = i + 1
        distance = 0
        while j < len(paragraphs) and distance <= max_heading_distance:
            if paragraphs[j]['class'] == 'good':
                paragraph['class'] = 'good'
                break
            distance += len(paragraphs[j]['text'])
            j += 1

def justext(html_text, stoplist, length_low=LENGTH_LOW_DEFAULT,
        length_high=LENGTH_HIGH_DEFAULT, stopwords_low=STOPWORDS_LOW_DEFAULT,
        stopwords_high=STOPWORDS_HIGH_DEFAULT, max_link_density=MAX_LINK_DENSITY_DEFAULT,
        max_good_distance=MAX_GOOD_DISTANCE_DEFAULT,
        max_heading_distance=MAX_HEADING_DISTANCE_DEFAULT, no_headings=NO_HEADINGS_DEFAULT,
        encoding=None, default_encoding=DEFAULT_ENCODING,
        enc_errors=DEFAULT_ENC_ERRORS):
    """
    Converts an HTML page into a list of classified paragraphs. Each paragraph
    is represented as a dictionary with the following attributes:
    
    text:
      Plain text content.
    
    cfclass:
      The context-free class -- class assigned by the context-free
      classification: 'good', 'bad', 'neargood' or 'short'.
    
    class:
      The final class: 'good' or 'bad'.
    
    heading:
      Set to True of the paragraph contains a heading, False otherwise.
    
    word_count:
      Number of words.
    
    linked_char_count:
      Number of characters inside links.

    link_density:
      linked_char_count / len(text)
            
    stopword_count:
      Number of stoplist words.
      
    stopword_density:
      stopword_count / word_count
    
    dom_path:
      A dom path to the paragraph in the originial HTML page.
    """
    root = preprocess(html_text, encoding=encoding,
        default_encoding=default_encoding, enc_errors=enc_errors)
    paragraphs = make_paragraphs(root)
    classify_paragraphs(paragraphs, stoplist, length_low, length_high,
        stopwords_low, stopwords_high, max_link_density, no_headings)
    revise_paragraph_classification(paragraphs, max_good_distance, max_heading_distance)
    return paragraphs

def lang2lang(lang):
    if lang in TAMERLON_SUPPORTED_LANGUAGES:
        return TAMERLON_SUPPORTED_LANGUAGES[lang]
    else:
        raise KeyError("Unsupported language %s in BTE" % lang)

def bte(html, lang):
    return "\n\n".join([
        p['text'].replace('<', '&lt;').replace('>', '&gt;').strip()
        for p in justext(html, get_stoplist(lang2lang(lang)))
        if p["class"] == "good"
    ])

def guess_title(html, lang):
    for p in justext(html, get_stoplist(lang2lang(lang))):
        if p["heading"]:
            return p['text'].replace('<', '&lt;').replace('>', '&gt;').strip()
    return None

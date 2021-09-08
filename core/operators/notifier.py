# coding:utf-8
import os
import smtplib
import datetime
from core.operators.html_test_runner import PARAMETRIZED
from core import settings
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


class Notifier:

    # 实现类必须重写ALIAS，DP runner通过该别名实例对象
    ALIAS = None

    def __init__(self, logger, result, report):
        self.result = result
        self.logger = logger
        self.result_root = report

    def analyze_test_run_result(self):
        """
        分析测试结果，并返回总览信息
        :return: 测试总览信息
        """
        success_count = self.result['detail'].success_count
        error_count = self.result['detail'].error_count
        failure_count = self.result['detail'].failure_count
        skip_count = len(self.result['detail'].skipped)
        total_count = success_count + error_count + failure_count + skip_count
        start = self.result['startTime']
        stop = datetime.datetime.now()
        seconds = (stop - start).seconds
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        summary = dict(
            result='PASS' if success_count == total_count else 'FAIL',
            success_count=success_count,
            success_rate=str(round((success_count / total_count) * 100)) + '%',
            error_count=error_count,
            failure_count=failure_count,
            skip_count=skip_count,
            total_count=total_count,
            start=str(start),
            stop=str(stop),
            duration='%02d:%02d:%02d' % (h, m, s))
        return summary

    def analyze_suite_result(self):
        """
        分析测试结果，并返回测试套件总览信息
        :return: 测试套件总览信息
        """
        r_map = {}
        classes = []
        suites = []
        for n, t, o, e in self.result['detail'].result:
            cls = t.__class__
            if cls not in r_map:
                r_map[cls] = []
                classes.append(cls)
            r_map[cls].append((n, t, o, e))
        for t, n in self.result['detail'].skipped:
            cls = t.__class__
            if cls not in r_map:
                r_map[cls] = []
                classes.append(cls)
            r_map[cls].append((-1, t, n, ''))
        sorted_result = [(cls, r_map[cls]) for cls in classes]
        for cid, (cls, cls_results) in enumerate(sorted_result):
            # 为每个测试类记录测试数据
            np = nf = ne = ns = 0
            for n, t, o, e in cls_results:
                if n == 0:
                    np += 1
                elif n == 1:
                    nf += 1
                elif n == -1:
                    ns += 1
                else:
                    ne += 1

            # 通过包、类名生成测试脚本名称
            if cls.__module__ == "__main__":
                name = cls.__name__
            # 类包名为PARAMETRIZED，既DDT生产的测试类
            elif cls.__module__.startswith(PARAMETRIZED):
                name = "%s.%s" % (str(cls.__bases__).split('\'')[1].split('.')[0], cls.__name__)
            else:
                name = "%s.%s" % (cls.__module__, cls.__name__)

            doc = cls.__doc__ and cls.__doc__.split("\n")[0] or ""
            desc = doc and '%s: %s' % (name, doc) or name
            suite = dict(
                result=ne > 0 and 'error' or nf > 0 and 'fail' or 'pass',
                desc=desc,
                count=np + nf + ne + ns,
                success=np,
                success_rate=str(round((np / (np + nf + ne + ns)) * 100)) + '%',
                fail=nf,
                error=ne,
                skip=ns,
                cid=u'c%s' % (cid + 1),
            )
            suites.append(suite)
        return suites

    def inform(self):
        # 实现类，需实现通知函数
        raise NotImplemented


class EmailNotifier(Notifier):

    # 可以通过 -n email 调用发送邮件通知
    ALIAS = 'email'

    HTML_SUMMARY_TEMPLATE = r'''
    <h2>DataPipeline 自动化测试报告</h2>
    <h3>结果总览</h3>
    <table style="width:100%%">
        <thead style="text-align:center; background-color:lightblue">
            <th>总数</th>
            <th>通过率</th>
            <th>通过</th>
            <th>失败</th>
            <th>错误</th>
            <th>跳过</th>
            <th>运行时间</th>
        </thead>
        <tbody style="text-align:center;">
            <tr>
                <td style="background-color:#EEEEE0">%(total_count)s</td>
                <td style="background-color:#EEEEE0">%(success_rate)s</td>
                <td style="background-color:#EEEEE0">%(success_count)s</td>
                <td style="background-color:#EEEEE0">%(failure_count)s</td>
                <td style="background-color:#EEEEE0">%(error_count)s</td>
                <td style="background-color:#EEEEE0">%(skip_count)s</td>
                <td style="background-color:#EEEEE0">%(duration)s</td>
            </tr>
        </tbody>
    </table>
    <hr>
    '''

    HTML_SUITES_TEMPLATE = r'''
    <h3>测试套件结果</h3>
    <table style="width:100%%">
        <thead style="text-align:center; background-color:lightblue">
            <th>测试套件/测试用例</th>
            <th>总数</th>
            <th>通过率</th>
            <th>通过</th>
            <th>失败</th>
            <th>错误</th>
            <th>跳过</th>
        </thead>
        <tbody>
            %(suites)s
        </tbody>
    </table>
    '''

    HTML_EACH_SUITE_TEMPLATE = r'''
    <tr>
        <td style="background-color:#EEEEE0">%(desc)s</td>
        <td style="text-align:center; background-color:#EEEEE0">%(count)s</td>
        <td style="text-align:center; background-color:#EEEEE0">%(success_rate)s</td>
        <td style="text-align:center; background-color:#EEEEE0">%(success)s</td>
        <td style="text-align:center; background-color:#EEEEE0">%(fail)s</td>
        <td style="text-align:center; background-color:#EEEEE0">%(error)s</td>
        <td style="text-align:center; background-color:#EEEEE0">%(skip)s</td>
    </tr>
    '''

    def _send_email_via_smtp(self, subject, body, attachments):
        self.logger.info('发送测试结果邮件...')
        msg = MIMEMultipart()
        msg['From'] = settings.EMAIL_NOTIFIER_USERNAME
        msg['To'] = ','.join(settings.EMAIL_TO_LIST)
        msg['CC'] = ','.join(settings.EMAIL_CC_LIST)
        msg['BCC'] = ','.join(settings.EMAIL_BCC_LIST)
        msg['Subject'] = subject
        body = body
        msg.attach(MIMEText(body, 'html'))
        if attachments:
            for attachment in attachments:
                filename = attachment['key']
                content = attachment['content']
                p = MIMEBase('application', 'octet-stream')
                p.set_payload(content, charset='utf-8')
                encoders.encode_base64(p)
                p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
                msg.attach(p)

        # send email
        with smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(settings.EMAIL_NOTIFIER_USERNAME, settings.EMAIL_NOTIFIER_PASSWORD)
            body = msg.as_string()
            smtp.sendmail(settings.EMAIL_NOTIFIER_USERNAME, settings.EMAIL_TO_LIST, body)
            self.logger.info('邮件发送成功')

    def _get_html_report(self):
        path = os.path.join(self.result_root, 'report.html')
        if not os.path.exists(path):
            self.logger.info('report.html 结果报告文件并未生成，请查看前置脚本错误')
            return {}
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        return {'key': 'Report.html', 'content': content}

    def _get_log(self):
        path = os.path.join(self.result_root, 'runner.log')
        if not os.path.exists(path):
            self.logger.info('runner.log 结果日志文件并未生成，请查看前置脚本错误')
            return {}
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        return {'key': 'Log.txt', 'content': content}

    def _generate_subject_body(self):
        test_run_result = self.analyze_test_run_result()
        suites_result = self.analyze_suite_result()
        subject = '[{0}] DataPipeline 自动化测试报告'.format(test_run_result['result'])
        body = self.HTML_SUMMARY_TEMPLATE % dict(
            total_count=test_run_result['total_count'],
            success_rate=test_run_result['success_rate'],
            success_count=test_run_result['success_count'],
            failure_count=test_run_result['failure_count'],
            error_count=test_run_result['error_count'],
            skip_count=test_run_result['skip_count'],
            duration=test_run_result['duration'],
        )
        rows = []
        for suite in suites_result:
            row = self.HTML_EACH_SUITE_TEMPLATE % dict(
                desc=suite['desc'],
                count=suite['count'],
                success_rate=suite['success_rate'],
                success=suite['success'],
                fail=suite['fail'],
                error=suite['error'],
                skip=suite['skip'],
            )
            rows.append(row)
        body += self.HTML_SUITES_TEMPLATE % {'suites': ''.join(rows)}
        return subject, body

    def inform(self):
        # 解析并拼接邮件主题，主体信息
        subject, body = self._generate_subject_body()

        # 加载测试报告、测试日志文件
        attachments = []
        report_dict = self._get_html_report()
        log_dict = self._get_log()
        attachments.append(report_dict)
        attachments.append(log_dict)

        # 发送邮件
        self._send_email_via_smtp(subject, body, attachments)

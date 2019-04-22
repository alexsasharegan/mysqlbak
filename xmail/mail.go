package xmail

import (
	"context"
	"fmt"
	"time"

	mailgun "github.com/mailgun/mailgun-go"
)

// MailConfig configures Mailgun.
type MailConfig struct {
	ApiKey string
	Domain string
	Sender string
	MailTo string
}

// Message is an email message.
type Message struct {
	Subject   string
	Body      string
	Recipient string
	apikey    string
	domain    string
	sender    string
}

// SetConfig saves the mailgun config.
func (m *Message) SetConfig(mc *MailConfig) {
	m.domain = mc.Domain
	m.apikey = mc.ApiKey
	m.sender = fmt.Sprintf("%s <noreply@%s>", mc.Sender, mc.Domain)
}

// Send an email with mailgun.
func Send(ctx context.Context, msg *Message) (mes string, id string, err error) {
	mg := mailgun.NewMailgun(msg.domain, msg.apikey)
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
	}

	m := mg.NewMessage(msg.sender, msg.Subject, msg.Body, msg.Recipient)
	return mg.Send(ctx, m)
}

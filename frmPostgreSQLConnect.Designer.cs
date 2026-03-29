using System.ComponentModel;
using System.Windows.Forms;

namespace NppDB.PostgreSQL
{
    partial class frmPostgreSQLConnect
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.txtServer = new System.Windows.Forms.TextBox();
            this.lblServer = new System.Windows.Forms.Label();
            this.lblPort = new System.Windows.Forms.Label();
            this.lblUsername = new System.Windows.Forms.Label();
            this.txtUsername = new System.Windows.Forms.TextBox();
            this.lblPassword = new System.Windows.Forms.Label();
            this.txtPassword = new System.Windows.Forms.TextBox();
            this.btnOK = new System.Windows.Forms.Button();
            this.btnCancel = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.txtDatabase = new System.Windows.Forms.TextBox();
            this.cbxShowPwd = new System.Windows.Forms.CheckBox();
            this.nmrPort = new System.Windows.Forms.NumericUpDown();
            this.lblConnName = new System.Windows.Forms.Label();
            this.txtConnName = new System.Windows.Forms.TextBox();
            this.btnTestConnection = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.nmrPort)).BeginInit();
            this.SuspendLayout();
            // 
            // txtServer
            // 
            this.txtServer.Location = new System.Drawing.Point(280, 78);
            this.txtServer.Name = "txtServer";
            this.txtServer.Size = new System.Drawing.Size(225, 20);
            this.txtServer.TabIndex = 11;
            // 
            // lblServer
            // 
            this.lblServer.AutoSize = true;
            this.lblServer.Location = new System.Drawing.Point(219, 81);
            this.lblServer.Name = "lblServer";
            this.lblServer.Size = new System.Drawing.Size(33, 13);
            this.lblServer.TabIndex = 991;
            this.lblServer.Text = "Host*";
            // 
            // lblPort
            // 
            this.lblPort.AutoSize = true;
            this.lblPort.Location = new System.Drawing.Point(219, 122);
            this.lblPort.Name = "lblPort";
            this.lblPort.Size = new System.Drawing.Size(30, 13);
            this.lblPort.TabIndex = 992;
            this.lblPort.Text = "Port*";
            // 
            // lblUsername
            // 
            this.lblUsername.AutoSize = true;
            this.lblUsername.Location = new System.Drawing.Point(219, 207);
            this.lblUsername.Name = "lblUsername";
            this.lblUsername.Size = new System.Drawing.Size(59, 13);
            this.lblUsername.TabIndex = 995;
            this.lblUsername.Text = "Username*";
            // 
            // txtUsername
            // 
            this.txtUsername.Location = new System.Drawing.Point(280, 204);
            this.txtUsername.Name = "txtUsername";
            this.txtUsername.Size = new System.Drawing.Size(225, 20);
            this.txtUsername.TabIndex = 14;
            // 
            // lblPassword
            // 
            this.lblPassword.AutoSize = true;
            this.lblPassword.Location = new System.Drawing.Point(219, 249);
            this.lblPassword.Name = "lblPassword";
            this.lblPassword.Size = new System.Drawing.Size(57, 13);
            this.lblPassword.TabIndex = 996;
            this.lblPassword.Text = "Password*";
            // 
            // txtPassword
            // 
            this.txtPassword.Location = new System.Drawing.Point(280, 246);
            this.txtPassword.Name = "txtPassword";
            this.txtPassword.Size = new System.Drawing.Size(225, 20);
            this.txtPassword.TabIndex = 15;
            // 
            // btnOK
            // 
            this.btnOK.Location = new System.Drawing.Point(280, 287);
            this.btnOK.Name = "btnOK";
            this.btnOK.Size = new System.Drawing.Size(75, 23);
            this.btnOK.TabIndex = 17;
            this.btnOK.Text = "OK";
            this.btnOK.UseVisualStyleBackColor = true;
            this.btnOK.Click += new System.EventHandler(this.btnOK_Click);
            // 
            // btnCancel
            // 
            this.btnCancel.Location = new System.Drawing.Point(431, 287);
            this.btnCancel.Name = "btnCancel";
            this.btnCancel.Size = new System.Drawing.Size(75, 23);
            this.btnCancel.TabIndex = 18;
            this.btnCancel.Text = "Cancel";
            this.btnCancel.UseVisualStyleBackColor = true;
            this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(219, 165);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(57, 13);
            this.label1.TabIndex = 994;
            this.label1.Text = "Database*";
            // 
            // txtDatabase
            // 
            this.txtDatabase.Location = new System.Drawing.Point(280, 162);
            this.txtDatabase.Name = "txtDatabase";
            this.txtDatabase.Size = new System.Drawing.Size(225, 20);
            this.txtDatabase.TabIndex = 13;
            // 
            // cbxShowPwd
            // 
            this.cbxShowPwd.AutoSize = true;
            this.cbxShowPwd.Location = new System.Drawing.Point(514, 249);
            this.cbxShowPwd.Name = "cbxShowPwd";
            this.cbxShowPwd.Size = new System.Drawing.Size(101, 17);
            this.cbxShowPwd.TabIndex = 16;
            this.cbxShowPwd.Text = "Show password";
            this.cbxShowPwd.UseVisualStyleBackColor = true;
            this.cbxShowPwd.CheckedChanged += new System.EventHandler(this.cbxShowPwd_CheckedChanged);
            // 
            // nmrPort
            // 
            this.nmrPort.Location = new System.Drawing.Point(280, 120);
            this.nmrPort.Maximum = new decimal(new int[] { 1000000, 0, 0, 0 });
            this.nmrPort.Name = "nmrPort";
            this.nmrPort.Size = new System.Drawing.Size(225, 20);
            this.nmrPort.TabIndex = 12;
            this.nmrPort.Value = new decimal(new int[] { 5432, 0, 0, 0 });
            // 
            // lblConnName
            // 
            this.lblConnName.AutoSize = true;
            this.lblConnName.Location = new System.Drawing.Point(184, 39);
            this.lblConnName.Name = "lblConnName";
            this.lblConnName.Size = new System.Drawing.Size(90, 13);
            this.lblConnName.TabIndex = 993;
            this.lblConnName.Text = "Connection name";
            // 
            // txtConnName
            // 
            this.txtConnName.Location = new System.Drawing.Point(280, 36);
            this.txtConnName.Name = "txtConnName";
            this.txtConnName.Size = new System.Drawing.Size(225, 20);
            this.txtConnName.TabIndex = 10;
            // 
            // btnTestConnection
            // 
            this.btnTestConnection.Location = new System.Drawing.Point(281, 327);
            this.btnTestConnection.Name = "btnTestConnection";
            this.btnTestConnection.Size = new System.Drawing.Size(225, 23);
            this.btnTestConnection.TabIndex = 19;
            this.btnTestConnection.Text = "Test connection";
            this.btnTestConnection.UseVisualStyleBackColor = true;
            this.btnTestConnection.Click += new System.EventHandler(this.btnTestConnection_Click);
            // 
            // frmPostgreSQLConnect
            // 
            this.AcceptButton = this.btnOK;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.btnTestConnection);
            this.Controls.Add(this.lblConnName);
            this.Controls.Add(this.txtConnName);
            this.Controls.Add(this.nmrPort);
            this.Controls.Add(this.cbxShowPwd);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.txtDatabase);
            this.Controls.Add(this.btnCancel);
            this.Controls.Add(this.btnOK);
            this.Controls.Add(this.lblPassword);
            this.Controls.Add(this.txtPassword);
            this.Controls.Add(this.lblUsername);
            this.Controls.Add(this.txtUsername);
            this.Controls.Add(this.lblPort);
            this.Controls.Add(this.lblServer);
            this.Controls.Add(this.txtServer);
            this.Name = "frmPostgreSQLConnect";
            this.Text = "PostgreSQL server connection";
            this.Load += new System.EventHandler(this.frmPassword_Load);
            ((System.ComponentModel.ISupportInitialize)(this.nmrPort)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();
        }

        #endregion

        private TextBox txtServer;
        private Label lblServer;
        private Label lblPort;
        private Label lblUsername;
        private TextBox txtUsername;
        private Label lblPassword;
        private TextBox txtPassword;
        private Button btnOK;
        private Button btnCancel;
        private Label label1;
        private TextBox txtDatabase;
        private CheckBox cbxShowPwd;
        private NumericUpDown nmrPort;
        private Label lblConnName;
        private TextBox txtConnName;
        private Button btnTestConnection;
    }
}
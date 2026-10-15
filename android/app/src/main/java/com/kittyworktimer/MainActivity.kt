package com.kittyworktimer

import android.Manifest
import android.annotation.SuppressLint
import android.app.Activity
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Bundle
import android.webkit.CookieManager
import android.webkit.PermissionRequest
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Toast
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat

class MainActivity : Activity() {
    private lateinit var webView: WebView
    private val serverBase = BuildConfig.SERVER_BASE_URL.trimEnd('/')
    private val captureRequestCode = 771
    private var pendingPermissionRequest: PermissionRequest? = null

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        if (!serverBase.startsWith("https://") || serverBase.contains("CHANGE-ME")) {
            Toast.makeText(this, "Configure SERVER_BASE_URL to your HTTPS server before building.", Toast.LENGTH_LONG).show()
            finish()
            return
        }
        webView = WebView(this)
        CookieManager.getInstance().setAcceptCookie(true)
        CookieManager.getInstance().setAcceptThirdPartyCookies(webView, false)
        webView.settings.javaScriptEnabled = true
        webView.settings.domStorageEnabled = true
        // Allow remote WebRTC video/audio tracks to start after the user has logged in.
        webView.settings.mediaPlaybackRequiresUserGesture = false
        webView.settings.allowFileAccess = false
        webView.settings.allowContentAccess = false
        webView.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean {
                val uri: Uri = request.url
                return uri.scheme != "https" || uri.host != Uri.parse(serverBase).host
            }

            override fun onPageFinished(view: WebView, url: String) {
                super.onPageFinished(view, url)
                if (Uri.parse(url).host != Uri.parse(serverBase).host) return
                val username = BuildConfig.DEFAULT_USERNAME
                val escaped = username.replace("\\", "\\\\").replace("'", "\\'")
                view.evaluateJavascript(
                    "(function(){const u=document.getElementById('username');if(u){u.value='$escaped';const p=document.getElementById('password');if(p)p.focus();}})();",
                    null
                )
            }
        }
        webView.webChromeClient = object : WebChromeClient() {
            override fun onPermissionRequest(request: PermissionRequest) {
                runOnUiThread {
                    val missing = arrayOf(Manifest.permission.CAMERA, Manifest.permission.RECORD_AUDIO)
                        .filter { ContextCompat.checkSelfPermission(this@MainActivity, it) != PackageManager.PERMISSION_GRANTED }
                    if (missing.isNotEmpty()) {
                        pendingPermissionRequest = request
                        ActivityCompat.requestPermissions(this@MainActivity, missing.toTypedArray(), captureRequestCode)
                    } else grantCaptureResources(request)
                }
            }
        }
        setContentView(webView)
        if (savedInstanceState != null) webView.restoreState(savedInstanceState) else webView.loadUrl("$serverBase/")
    }

    private fun grantCaptureResources(request: PermissionRequest) {
        request.grant(request.resources.filter {
            it == PermissionRequest.RESOURCE_VIDEO_CAPTURE || it == PermissionRequest.RESOURCE_AUDIO_CAPTURE
        }.toTypedArray())
    }

    override fun onRequestPermissionsResult(requestCode: Int, permissions: Array<out String>, grantResults: IntArray) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        if (requestCode == captureRequestCode) {
            val request = pendingPermissionRequest
            pendingPermissionRequest = null
            if (grantResults.isNotEmpty() && grantResults.all { it == PackageManager.PERMISSION_GRANTED }) {
                request?.let { grantCaptureResources(it) }
            } else request?.deny()
        }
    }

    override fun onSaveInstanceState(outState: Bundle) {
        webView.saveState(outState)
        super.onSaveInstanceState(outState)
    }

    @Deprecated("Deprecated in Java")
    override fun onBackPressed() {
        if (::webView.isInitialized && webView.canGoBack()) webView.goBack() else super.onBackPressed()
    }
}
